import SQLiteModuleFactory from '../dist/wa-sqlite.mjs';
import * as SQLite from '../src/sqlite-api.js';
import { SyncAccessHandleVFS } from "../src/examples/SyncAccessHandleVFS";
import { tag } from "../src/examples/tag.js";

console.log('worker started');

(async function() {
  const rootHandle = await navigator.storage.getDirectory();
  const dirHandle = await rootHandle.getDirectoryHandle('foo')
    .catch(() => null);
  if (dirHandle) {
    // for await (const filename of dirHandle.keys()) {
    //   console.log(filename);
    // }

    // await rootHandle.removeEntry('foo', { recursive: true });
    // console.log('directory "foo" deleted');
  }

  await new Promise(resolve => setTimeout(resolve, 2000));

  globalThis.vfs = new SyncAccessHandleVFS('/foo');
  await globalThis.vfs.reset();
  if (globalThis.vfs.getCapacity() < 6) {
    await globalThis.vfs.addCapacity(6 - globalThis.vfs.getCapacity());
  }

  const module = await SQLiteModuleFactory();
  const sqlite3 = SQLite.Factory(module);
  // @ts-ignore
  sqlite3.vfs_register(globalThis.vfs, true);

  const db = await sqlite3.open_v2(
    'foo.db',
    SQLite.SQLITE_OPEN_CREATE | SQLite.SQLITE_OPEN_READWRITE | SQLite.SQLITE_OPEN_URI,
    'sync-access-handle');
  
  globalThis.sql = tag(sqlite3, db);
})();

