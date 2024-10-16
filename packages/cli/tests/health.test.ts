import { expect, test } from "bun:test";
import Server from "./server.ts";

test("health", async () => {
	await using server = await Server.start({
		database: {
			kind: "sqlite",
			connections: 1,
		},
	});
	const health = await server.tg`health`.text();
	expect(health).toMatchSnapshot();
});
