use ../../test.nu *

# Disposing a connected handle cancels its process and closes the connection.
let server = server spawn
let path = artifact {
    tangram.ts: '
        export default async function () {
            let child = await tg.spawn`read line`.stdin("pipe").stdout("null").stderr("null").sandbox().connection("run");
            await child[Symbol.asyncDispose]();
            tg.assert((await child.wait()).exit === 1);
            tg.assert(child.connection === null);
            return "ok";
        }
    '
}
let output = tg run $path | from json
assert ($output == "ok")

let output = tg run --sandbox $path | from json
assert ($output == "ok")
