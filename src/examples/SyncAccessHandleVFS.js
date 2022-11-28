// Copyright 2022 Roy T. Hashimoto. All Rights Reserved.
import * as VFS from '../VFS.js';

const BLOCK_SIZE = 4096;

const HEADER_MAX_PATH_SIZE = 512;
const HEADER_DIGEST_SIZE = 8;
const HEADER_OFFSET_PATH = 0;
const HEADER_OFFSET_DIGEST = HEADER_MAX_PATH_SIZE;
const HEADER_OFFSET_DATA = HEADER_OFFSET_DIGEST + HEADER_DIGEST_SIZE;

function log(...args) {
  console.debug(...args);
}

export class SyncAccessHandleVFS extends VFS.Base {
  #ready;
  #directoryHandle;

  #mapPathToAccessHandle = new Map();
  #mapAccessHandleToName = new Map();

  #mapIdToFile = new Map();

  constructor(directoryPath) {
    super();
    this.#ready = this.#initialize(directoryPath);
  }

  get name() { return 'sync-access-handle'; }

  xOpen(name, fileId, flags, pOutFlags) {
    log(`xOpen ${name} ${fileId} 0x${flags.toString(16)}`);
    try {
      const path = name ? this.#getPath(name) : Math.random().toString(36);
      let accessHandle = this.#mapPathToAccessHandle.get(path);
      if (!accessHandle && (flags & VFS.SQLITE_OPEN_CREATE)) {
        if (this.getSize() < this.getCapacity()) {
          ([accessHandle] = this.#mapAccessHandleToName.keys());
          this.#setAssociatedPath(accessHandle, path);
        } else {
          throw new Error('cannot create file');
        }
      }
      if (!accessHandle) {
        throw new Error('file not found');
      }
      this.#mapPathToAccessHandle.set(path, accessHandle);

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
    log(`xClose ${file.filename}`);

    this.#mapIdToFile.delete(fileId);
    if (file.flags & VFS.SQLITE_OPEN_DELETEONCLOSE) {
      this.#deletePath(file.path);
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
    log(`xSync ${file.filename} ${flags}`);

    file.accessHandle.flush();
    return VFS.SQLITE_OK;
  }

  xFileSize(fileId, pSize64) {
    const file = this.#mapIdToFile.get(fileId);
    log(`xFileSize ${file.path}`);

    const size = file.accessHandle.getSize() - HEADER_OFFSET_DATA;
    pSize64.set(size);
    return VFS.SQLITE_OK;
  }

  xSectorSize(fileId) {
    log('xSectorSize', BLOCK_SIZE);
    return BLOCK_SIZE;
  }

  xDeviceCharacteristics(fileId) {
    log('xDeviceCharacteristics');
    return VFS.SQLITE_IOCAP_SAFE_APPEND |
           VFS.SQLITE_IOCAP_SEQUENTIAL |
           VFS.SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN;
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

  ready() {
    return this.#ready;
  }

  getSize() {
    return this.#mapPathToAccessHandle.size;
  }

  getCapacity() {
    return this.#mapAccessHandleToName.size;
  }

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
  }

  async removeCapacity(n) {
    let nRemoved = 0;
    for (const [accessHandle, name] of this.#mapAccessHandleToName) {
      if (nRemoved == n || this.getSize() === this.getCapacity()) return;

      await accessHandle.close();
      await this.#directoryHandle.removeEntry(name);
      this.#mapAccessHandleToName.delete(accessHandle);
      ++nRemoved;
    }
  }

  async #initialize(directoryPath) {
    // All files are stored in a single directory.
    let handle = await navigator.storage.getDirectory();
    for (const d of directoryPath.split('/')) {
      if (d) {
        handle = await handle.getDirectoryHandle(d, { create: true });
      }
    }
    this.#directoryHandle = handle;

    await this.#acquireAccessHandles();
  }

  async #acquireAccessHandles() {
    /** @type {[any, string][]} */ const tuplesWithPath = [];
    /** @type {[any, string][]} */ const tuplesWithoutPath = [];

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
    const encodedPath = new Uint8Array(HEADER_MAX_PATH_SIZE);
    const encodedResult = new TextEncoder().encodeInto(path, encodedPath);
    if (encodedResult.written >= encodedPath.byteLength) {
      throw new Error('path too long');
    }

    const digest = this.#computeDigest(encodedPath);
    accessHandle.write(encodedPath, { at: HEADER_OFFSET_PATH });
    accessHandle.write(digest, { at: HEADER_OFFSET_DIGEST });
    if (!path) {
      accessHandle.truncate(HEADER_OFFSET_DATA);
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
      this.#setAssociatedPath(accessHandle, '');
      this.#mapPathToAccessHandle.delete(path);

      // Move accessHandle to the front of #mapAccessHandleToName.
      const name = this.#mapAccessHandleToName.get(accessHandle);
      this.#mapAccessHandleToName.delete(accessHandle);
      this.#mapAccessHandleToName.set(accessHandle, name);
    }
  }
}