// Copyright 2022 Roy T. Hashimoto. All Rights Reserved.
import * as VFS from '../VFS.js';

const SECTOR_SIZE = 4096;

// Each OPFS file begins with a fixed-size header with metadata. The
// contents of the file follow immediately after the header.
const HEADER_MAX_PATH_SIZE = 512;
const HEADER_DIGEST_SIZE = 8;
const HEADER_OFFSET_PATH = 0;
const HEADER_OFFSET_DIGEST = HEADER_MAX_PATH_SIZE;
const HEADER_OFFSET_DATA = HEADER_OFFSET_DIGEST + HEADER_DIGEST_SIZE;

function log(...args) {
  // console.debug(...args);
}

/**
 * This VFS uses the updated Access Handle API with all synchronous methods
 * on FileSystemSyncAccessHandle (instead of just read and write). It will
 * work with the regular SQLite WebAssembly build, i.e. the one without
 * Asyncify.
 */
export class SyncAccessHandleVFS extends VFS.Base {
  // All the OPFS files the VFS uses are contained in one flat directory
  // specified in the constructor. No other files should be written here.
  #directoryPath;
  #directoryHandle;

  // The OPFS files all have randomly-generated names that do not match
  // the SQLite files whose data they contain. This map links those names
  // with their respective OPFS access handles. In this map, all the OPFS
  // files that are not yet associated with a SQLite file precede the
  // OPFS files that are associated with a SQLite file.
  #mapAccessHandleToName = new Map();

  // When a SQLite file is associated with an OPFS file, that association
  // is kept in this map.
  #mapPathToAccessHandle = new Map();

  #mapIdToFile = new Map();

  constructor(directoryPath) {
    super();
    this.#directoryPath = directoryPath;
  }

  get name() { return 'sync-access-handle'; }

  xOpen(name, fileId, flags, pOutFlags) {
    log(`xOpen ${name} ${fileId} 0x${flags.toString(16)}`);
    try {
      // First try to open a path that already exists in the file system.
      const path = name ? this.#getPath(name) : Math.random().toString(36);
      let accessHandle = this.#mapPathToAccessHandle.get(path);
      if (!accessHandle && (flags & VFS.SQLITE_OPEN_CREATE)) {
        // File not found so try to create it.
        if (this.getSize() < this.getCapacity()) {
          // Choose an unassociated OPFS file from the pool.
          ([accessHandle] = this.#mapAccessHandleToName.keys());
          this.#setAssociatedPath(accessHandle, path);
        } else {
          // Out of unassociated files. This can be fixed with addCapacity().
          throw new Error('cannot create file');
        }
      }
      if (!accessHandle) {
        throw new Error('file not found');
      }
      this.#mapPathToAccessHandle.set(path, accessHandle);

      // Subsequent methods are only passed the fileId, so make sure we have
      // a way to get the file resources.
      const file = {
        path,
        flags,
        accessHandle
      }
      this.#mapIdToFile.set(fileId, file);

      pOutFlags.set(0);
      return VFS.SQLITE_OK;
    } catch (e) {
      console.error(e.message);
      return VFS.SQLITE_CANTOPEN;
    }
  }

  xClose(fileId) {
    const file = this.#mapIdToFile.get(fileId);
    if (file) {
      log(`xClose ${file.path}`);

      this.#mapIdToFile.delete(fileId);
      if (file.flags & VFS.SQLITE_OPEN_DELETEONCLOSE) {
        this.#deletePath(file.path);
      }
    }
    return VFS.SQLITE_OK;
  }

  xRead(fileId, pData, iOffset) {
    const file = this.#mapIdToFile.get(fileId);
    log(`xRead ${file.path} ${pData.size} ${iOffset}`);

    const nBytes = file.accessHandle.read(pData.value, { at: HEADER_OFFSET_DATA + iOffset });
    if (nBytes < pData.size) {
      pData.value.fill(0, nBytes, pData.size);
      return VFS.SQLITE_IOERR_SHORT_READ;
    }
    return VFS.SQLITE_OK;
  }

  xWrite(fileId, pData, iOffset) {
    const file = this.#mapIdToFile.get(fileId);
    log(`xWrite ${file.path} ${pData.size} ${iOffset}`);

    const nBytes = file.accessHandle.write(pData.value, { at: HEADER_OFFSET_DATA + iOffset });
    return nBytes === pData.size ? VFS.SQLITE_OK : VFS.SQLITE_IOERR;
  }

  xTruncate(fileId, iSize) {
    const file = this.#mapIdToFile.get(fileId);
    log(`xTruncate ${file.path} ${iSize}`);

    file.accessHandle.truncate(HEADER_OFFSET_DATA + iSize);
    return VFS.SQLITE_OK;
  }

  xSync(fileId, flags) {
    const file = this.#mapIdToFile.get(fileId);
    log(`xSync ${file.path} ${flags}`);

    file.accessHandle.flush();
    return VFS.SQLITE_OK;
  }

  xFileSize(fileId, pSize64) {
    const file = this.#mapIdToFile.get(fileId);
    const size = file.accessHandle.getSize() - HEADER_OFFSET_DATA;
    log(`xFileSize ${file.path} ${size}`);
    pSize64.set(size);
    return VFS.SQLITE_OK;
  }

  xSectorSize(fileId) {
    log('xSectorSize', SECTOR_SIZE);
    return SECTOR_SIZE;
  }

  xDeviceCharacteristics(fileId) {
    log('xDeviceCharacteristics');
    return VFS.SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN;
  }

  xAccess(name, flags, pResOut) {
    log(`xAccess ${name} ${flags}`);
    const path = this.#getPath(name);
    if (this.#mapPathToAccessHandle.has(path)) {
      pResOut.set(1);
    } else {
      pResOut.set(0);
    }
    return VFS.SQLITE_OK;
  }

  xDelete(name, syncDir) {
    log(`xDelete ${name} ${syncDir}`);
    const path = this.#getPath(name);
    this.#deletePath(path);
    return VFS.SQLITE_OK;
  }

  async close() {
    await this.#releaseAccessHandles();
  }

  /**
   * Release and reacquire all OPFS access handles. This must be called
   * and awaited before any SQLite call that uses the VFS and also before
   * any capacity changes.
   */
  async reset() {
    // All files are stored in a single directory.
    let handle = await navigator.storage.getDirectory();
    for (const d of this.#directoryPath.split('/')) {
      if (d) {
        handle = await handle.getDirectoryHandle(d, { create: true });
      }
    }
    this.#directoryHandle = handle;

    await this.#releaseAccessHandles();
    await this.#acquireAccessHandles();
  }

  /**
   * Returns the number of SQLite files in the file system.
   * @returns {number}
   */
  getSize() {
    return this.#mapPathToAccessHandle.size;
  }

  /**
   * Returns the maximum number of SQLite files the file system can hold.
   * @returns {number}
   */
  getCapacity() {
    return this.#mapAccessHandleToName.size;
  }

  /**
   * Increase the capacity of the file system by n.
   * @param {number} n 
   * @returns {Promise<number>} 
   */
  async addCapacity(n) {
    /** @type {[any, string][]} */ const newEntries = [];
    for (let i = 0; i < n; ++i) {
      const name = Math.random().toString(36).replace('0.', '');
      const handle = await this.#directoryHandle.getFileHandle(name, { create: true });
      const accessHandle = await handle.createSyncAccessHandle();
      newEntries.push([accessHandle, name]);

      this.#setAssociatedPath(accessHandle, '');
    }

    // Insert new entries at the front of #mapAccessHandleToName.
    this.#mapAccessHandleToName = new Map([...newEntries, ...this.#mapAccessHandleToName]);
    return n;
  }

  /**
   * Decrease the capacity of the file system by n. The capacity cannot be
   * decreased to fewer than the current number of SQLite files in the
   * file system.
   * @param {number} n 
   * @returns {Promise<number>}
   */
  async removeCapacity(n) {
    let nRemoved = 0;
    for (const [accessHandle, name] of this.#mapAccessHandleToName) {
      if (nRemoved == n || this.getSize() === this.getCapacity()) return nRemoved;

      await accessHandle.close();
      await this.#directoryHandle.removeEntry(name);
      this.#mapAccessHandleToName.delete(accessHandle);
      ++nRemoved;
    }
  }

  async #acquireAccessHandles() {
    /** @type {[any, string][]} */ const tuplesWithPath = [];
    /** @type {[any, string][]} */ const tuplesWithoutPath = [];

    // Enumerate all the files in the directory.
    // @ts-ignore
    for await (const [name, handle] of this.#directoryHandle) {
      if (handle.kind === 'file') {
        const accessHandle = await handle.createSyncAccessHandle();

        const path = this.#getAssociatedPath(accessHandle);
        if (path) {
          this.#mapPathToAccessHandle.set(path, accessHandle);
          tuplesWithPath.push([accessHandle, name]);
        } else {
          tuplesWithoutPath.push([accessHandle, name]);
        }
        console.debug(name, path);
      }
    }

    this.#mapAccessHandleToName = new Map([...tuplesWithoutPath, ...tuplesWithPath]);
  }

  #releaseAccessHandles() {
    for (const accessHandle of this.#mapAccessHandleToName.keys()) {
      accessHandle.close();
    }
    this.#mapAccessHandleToName.clear();
    this.#mapPathToAccessHandle.clear();
  }

  /**
   * 
   * // @param {FileSystemSyncAccessHandle} accessHandle 
   * @returns {string}
   */
  #getAssociatedPath(accessHandle) {
    // Read the path and digest of the path from the file.
    const encodedPath = new Uint8Array(HEADER_MAX_PATH_SIZE);
    accessHandle.read(encodedPath, { at: HEADER_OFFSET_PATH })

    const fileDigest = new Uint32Array(HEADER_DIGEST_SIZE / 4);
    accessHandle.read(fileDigest, { at: HEADER_OFFSET_DIGEST });

    // Verify the digest.
    const computedDigest = this.#computeDigest(encodedPath);
    if (fileDigest.every((value, i) => value === computedDigest[i])) {
      // Good digest. Decode the null-terminated path string.
      const pathBytes = encodedPath.findIndex(value => value === 0);
      return new TextDecoder().decode(encodedPath.subarray(0, pathBytes));
    } else {
      // Bad digest. Repair this header.
      this.#setAssociatedPath(accessHandle, '');
      return '';
    }
  }

  /**
   * // @param {FileSystemSyncAccessHandle} accessHandle 
   * @param {string} path
   */
  #setAssociatedPath(accessHandle, path) {
    // Convert the path string to UTF-8 and get the digest.
    const encodedPath = new Uint8Array(HEADER_MAX_PATH_SIZE);
    const encodedResult = new TextEncoder().encodeInto(path, encodedPath);
    if (encodedResult.written >= encodedPath.byteLength) {
      throw new Error('path too long');
    }
    const digest = this.#computeDigest(encodedPath);

    // Write the OPFS file header.
    accessHandle.write(encodedPath, { at: HEADER_OFFSET_PATH });
    accessHandle.write(digest, { at: HEADER_OFFSET_DIGEST });

    if (!path) {
      // This OPFS file doesn't represent any SQLite file so it doesn't
      // need to keep any data.
      accessHandle.truncate(HEADER_OFFSET_DATA);

      // This OPFS file is now unassociated, so move it to the front
      // of #mapAccessHandleToName.
      const name = this.#mapAccessHandleToName.get(accessHandle);
      if (name) {
        this.#mapAccessHandleToName.delete(accessHandle);
        this.#mapAccessHandleToName = new Map(
          [[accessHandle, name],
          ...this.#mapAccessHandleToName]);
      }
    }
    accessHandle.flush();

    if (path) {
      // Move associated access handles to the end of #mapAccessHandleToName.
      const name = this.#mapAccessHandleToName.get(accessHandle);
      if (name) {
        this.#mapAccessHandleToName.delete(accessHandle);
        this.#mapAccessHandleToName.set(accessHandle, name);
      }
    }
  }

  /**
   * We need a synchronous digest function so can't use WebCrypto.
   * Adapted from https://github.com/bryc/code/blob/master/jshash/experimental/cyrb53.js
   * @param {Uint8Array} corpus 
   * @returns {ArrayBuffer}
   */
  #computeDigest(corpus) {
    let h1 = 0xdeadbeef;
    let h2 = 0x41c6ce57;
    
    for (const value of corpus) {
      h1 = Math.imul(h1 ^ value, 2654435761);
      h2 = Math.imul(h2 ^ value, 1597334677);
    }
    
    h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
    h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
    
    return new Uint32Array([h1 >>> 0, h2 >>> 0]);
  };
  
  /**
   * @param {string|URL} nameOrURL
   * @returns {string}
   */
  #getPath(nameOrURL) {
    const url = typeof nameOrURL === 'string' ?
      new URL(nameOrURL, 'file://localhost/') :
      nameOrURL;
    return url.pathname;
  }

  #deletePath(path) {
    const accessHandle = this.#mapPathToAccessHandle.get(path);
    if (accessHandle) {
      // Un-associate the SQLite path from the OPFS file.
      this.#setAssociatedPath(accessHandle, '');
      this.#mapPathToAccessHandle.delete(path);
    }
  }
}