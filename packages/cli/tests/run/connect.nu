use ../../test.nu *

# A retained connection carries stdin, independent output readers, and repeated waits in one HTTP request.
let server = server spawn
let path = artifact {
    tangram.ts: '
        export default async function () {
            let requests: string[] = [];
            let send = tg.client.send.bind(tg.client);
            tg.client.send = (request) => {
                if (request.uri.path?.startsWith("/processes/")) requests.push(request.uri.path);
                return send(request);
            };
            let process = await tg.spawn`read line; echo "out:$line"; echo "err:$line" >&2`
                .stdio("pipe").sandbox().connection("run");
            let [, stdout, stderr, wait] = await Promise.all([
                process.stdin.writeAll(tg.encoding.utf8.encode("hello\n")),
                process.stdout.text(), process.stderr.text(), process.wait(),
            ]);
            tg.assert(stdout === "out:hello\n");
            tg.assert(stderr === "err:hello\n");
            tg.assert(wait.exit === 0);
            tg.assert((await process.wait()).exit === 0);
            tg.assert(requests.length === 1 && requests[0] === "/processes/connect");
            return "ok";
        }
    '
}
let output = tg run $path | from json
assert ($output == "ok")

let output = tg run --sandbox $path | from json
assert ($output == "ok")
