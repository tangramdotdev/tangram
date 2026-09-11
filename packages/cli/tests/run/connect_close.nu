use ../../test.nu *

# Closing a subscription and failing an input iterator leave the connection usable.
let server = server spawn
let path = artifact {
    tangram.ts: '
        export default async function () {
            let process = await tg.spawn`echo ready; read line; echo "$line"`.stdin("pipe").stdout("log").stderr("null").sandbox().connection("run");
            tg.assert(typeof process.id === "string");
            for (let i = 0; i < 70; i++) {
                let read = await process.readStdio({ streams: ["stdout"], position: 0 });
                let chunk = await read.next();
                tg.assert(!chunk.done);
                await read.return?.();
            }
            let rejected = false;
            try {
                await process.connection!.write(process.id, { streams: ["stdin"] }, (async function* (): AsyncIterableIterator<tg.Process.Stdio.Chunk> {
                    throw new Error("the input failed");
                })());
            } catch (error) {
                rejected = String(error).includes("the input failed");
            }
            tg.assert(rejected);
            let read = await process.readStdio({ streams: ["stdout"], position: 0 });
            let reading = (async () => {
                let text = "";
                for await (let chunk of read) text += tg.encoding.utf8.decode(chunk.bytes);
                return text;
            })();
            await process.stdin.writeAll(tg.encoding.utf8.encode("done\n"));
            await process.stdin.close();
            let text = await reading;
            tg.assert(text === "ready\ndone\n");
            tg.assert((await process.wait()).exit === 0);
            return "ok";
        }
    '
}
let output = tg run $path | from json
assert ($output == "ok")

let output = tg run --sandbox $path | from json
assert ($output == "ok")
