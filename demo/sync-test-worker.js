import { SyncAccessHandleVFS } from "../src/examples/SyncAccessHandleVFS";

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

  globalThis.vfs = new SyncAccessHandleVFS('/foo');
})();

