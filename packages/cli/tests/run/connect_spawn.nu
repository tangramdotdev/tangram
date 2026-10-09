use ../lib/test.nu *

# Spawn mode accepts a finite request body and returns without waiting for process completion.
let local = server spawn
let path = artifact {
	tangram.ts: '
		export default async function () {
			let send = tg.client.send.bind(tg.client);
			tg.client.send = (request) => {
				if (request.uri.path === "/processes/connect") {
                    tg.assert(request.headers.get("x-tg-arg-in-body") === "true");
                    let input = request.body![Symbol.asyncIterator]();
                    request.body = new tg.Request({
                        ...request,
                        body: (async function* () {
                            let first = await input.next();
                            tg.assert(!first.done);
                            yield first.value;
                            await input.return?.();
                        })(),
                    }).body;
				}
				return send(request);
			};
			let process = await tg.spawn`read line`
				.stdin("pipe")
				.stdout("null")
				.stderr("null")
				.sandbox();
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
