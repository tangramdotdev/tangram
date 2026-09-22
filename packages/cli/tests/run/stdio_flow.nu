use ../lib/test.nu *

# Piped writes exceed one flow window while an idle stdout reader leaves stderr usable.
let server = server spawn
let path = artifact {
    tangram.ts: '
        export default async function () {
            for (let connection of [false, true]) {
                let spawn = tg.spawn`printf stderr >&2; while IFS= read -r line; do printf "%s\n" "$line"; done`.stdio("pipe").sandbox();
                if (connection) spawn = spawn.connection("run");
                let process = await spawn;
                let bytes = tg.encoding.utf8.encode(("x".repeat(4095) + "\n").repeat(1024));
                let writing = process.stdin.writeAll(bytes);
                let first = await process.stdout.read();
                tg.assert(first !== null);
                let stderr = await process.stderr.read();
                tg.assert(stderr !== null && tg.encoding.utf8.decode(stderr) === "stderr");
                let rest = await process.stdout.readAll();
                await writing;
                tg.assert(await process.stderr.text() === "");
                tg.assert(first.length + rest.length === bytes.length);
                tg.assert((await process.wait()).exit === 0);
            }
            return "ok";
        }
    '
}
let output = tg run $path | from json
assert ($output == "ok")
