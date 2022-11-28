// Copyright 2022 Roy T. Hashimoto. All Rights Reserved.
import * as VFS from '../VFS.js';

const BLOCK_SIZE = 4096;

const HEADER_MAX_PATH_SIZE = 512;
const HEADER_DIGEST_SIZE = 8;
const HEADER_OFFSET_PATH = 0;
const HEADER_OFFSET_DIGEST = HEADER_MAX_PATH_SIZE;
const HEADER_OFFSET_DATA = HEADER_OFFSET_DIGEST + HEADER_DIGEST_SIZE;

function log(...args) {
  // console.debug(...args);
}

export class SyncAccessHandleVFS extends VFS.Base {
  #ready;
  #directoryHandle;

  #mapPathToAccessHandle = new Map();
  #mapAccessHandleToName = new Map();

  constructor(directoryPath) {
    super();
    this.#ready = this.#initialize(directoryPath);
  }

  get name() { return 'sync-access-handle'; }

  async close() {
    await this.#releaseAccessHandles();
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
}