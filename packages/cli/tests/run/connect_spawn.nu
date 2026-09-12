use ../../test.nu *

# Spawn mode accepts a finite request body and returns without waiting for process completion.
let server = server spawn
let path = artifact {
    tangram.ts: '
        export default async function () {
            let send = tg.client.send.bind(tg.client);
            tg.client.send = (request) => {
                if (request.uri.path === "/processes/connect") {
                    let input = request.body!.sse();
                    request.body = new tg.Request({
                        ...request,
                        body: (async function* () {
                            let first = await input.next();
                            tg.assert(!first.done);
                            let opening = JSON.parse(first.value.data);
                            tg.assert(opening.arg.value.target.value.mode === "spawn");
                            yield [`event: ${first.value.event}`, `data: ${first.value.data}`, "", ""].join("\n");
                        })(),
                    }).body;
                }
                return send(request);
            };
            let process = await tg.spawn`read line`.stdin("pipe").stdout("null").stderr("null").sandbox();
            await process.detach();
            tg.client.send = send;
            await process.cancel();
            await process.wait();
            return "ok";
        }
    '
}
let output = tg run $path | from json
assert ($output == "ok")

let output = tg run --sandbox $path | from json
assert ($output == "ok")
