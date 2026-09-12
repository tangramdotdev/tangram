use ../../test.nu *

# Existing processes support a combined initial log subscription without a separate read request.
let server = server spawn
let path = artifact {
    tangram.ts: '
        export default async function () {
            let process = await tg.spawn`echo stdout; echo stderr >&2`.stdin("null").stdout("log").stderr("log").sandbox();
            await process.detach();
            tg.assert(typeof process.id === "string");
            let reads: tg.Process.Stdio.Read.Arg[] = [{ streams: ["stdout", "stderr"] }];
            let connected = await tg.Process.connect(process.id, { lease: process.lease, location: process.location, tokens: process.tokens, reads });
            let output = { stdout: "", stderr: "" };
            for await (let chunk of await connected.readStdio(reads[0]!)) {
                tg.assert(chunk.stream === "stdout" || chunk.stream === "stderr");
                output[chunk.stream] += tg.encoding.utf8.decode(chunk.bytes);
            }
            tg.assert(output.stdout === "stdout\n");
            tg.assert(output.stderr === "stderr\n");
            tg.assert((await connected.wait()).exit === 0);
            return "ok";
        }
    '
}
let output = tg run $path | from json
assert ($output == "ok")

let output = tg run --sandbox $path | from json
assert ($output == "ok")
