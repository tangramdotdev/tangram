import { test } from "bun:test";
import Server from "../server.ts";
import { directory } from "../util.ts";

test("push object to remote", async () => {
	await using remote = await Server.start();
	await using server = await Server.start({
		remotes: { default: { url: remote.url } },
	});
	let dir = await directory({
		foo: {
			"tangram.ts": `
				export default tg.target(() => "Hello, World");
			`,
		},
	});
	const id = await server.tg`checkin ${dir}/foo`.text().then((t) => t.trim());
	await server.tg`push ${id}`.quiet();
	await remote.tg`get ${id}`.quiet();
});
