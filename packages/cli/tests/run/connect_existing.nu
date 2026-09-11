use ../../test.nu *

# Detaching disarms cancellation before the stream closes, and connecting by ID does not spawn again.
let server = server spawn
let path = artifact {
    tangram.ts: '
        export default async function () {
            let process = await tg.spawn`read line; echo "$line"`.stdin("pipe").stdout("pipe").stderr("null").sandbox().connection("run");
            await process.detach();
            tg.assert(typeof process.id === "string");
            let connected = await tg.Process.connect(process.id, {
                lease: process.lease, location: process.location, tokens: process.tokens,
                reads: [{ streams: ["stdout"] }],
            });
            let [, text] = await Promise.all([
                connected.stdin.writeAll(tg.encoding.utf8.encode("attached\n")),
                connected.stdout.text(),
            ]);
            tg.assert(text === "attached\n");
            tg.assert((await connected.wait()).exit === 0);
            let detached = await tg.spawn`read line`.stdin("pipe").stdout("inherit").stderr("inherit").sandbox().connection("run");
            await detached.detach();
            await detached.stdin.writeAll(tg.encoding.utf8.encode("done\n"));
            await detached.output();
            return "ok";
        }
    '
}
let output = tg run $path | from json
assert ($output == "ok")

let output = tg run --sandbox $path | from json
assert ($output == "ok")
