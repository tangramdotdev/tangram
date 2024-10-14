import { expect, test } from "bun:test";
import Server from "./server.ts";

test("health", async () => {
	await using server = await Server.start();
	const health = await server.tg`server health`.text();
	expect(health).toMatchSnapshot();
});
