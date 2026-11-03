import { strict as assert } from "node:assert";
import { once } from "node:events";
import { createServer, type ServerHttp2Session } from "node:http2";
import { test } from "node:test";
import * as tg from "../src/index.ts";
import { Client } from "../src/client.ts";

const id = "blb_010000000000000000000000000000000000000000000000000000";
const retry = { backoff: 0, jitter: 0, maxDelay: 0, maxRetries: 2 };

test("buffered uploads replay after a connection failure while streaming uploads fail", async () => {
	const utf8 = tg.encoding.utf8;
	tg.encoding.utf8 = {
		decode: (value) => new TextDecoder().decode(value),
		encode: (value) => new TextEncoder().encode(value),
	};
	try {
		for (const value of ["héllo", new TextEncoder().encode("héllo"), null]) {
			const server = createServer();
			const sessions = new Set<ServerHttp2Session>();
			const bodies: Buffer[] = [];
			server.on("session", (session) => {
				sessions.add(session);
				session.on("error", () => {});
			});
			server.on("stream", (stream) => {
				const chunks: Buffer[] = [];
				stream.on("error", () => {});
				stream.on("data", (chunk: Buffer) => chunks.push(chunk));
				stream.on("end", () => {
					bodies.push(Buffer.concat(chunks));
					if (bodies.length === 1) {
						stream.session!.destroy();
						return;
					}
					stream.respond({
						":status": 200,
						"content-type": "application/json",
					});
					stream.end(JSON.stringify({ blob: id }));
				});
			});
			server.listen(0, "127.0.0.1");
			await once(server, "listening");
			const address = server.address();
			assert(address !== null && typeof address === "object");
			const client = new Client({
				url: `http://127.0.0.1:${address.port}`,
				retry,
				token: "",
			});
			try {
				if (value === null) {
					async function* chunks() {
						yield new TextEncoder().encode("héllo");
					}
					await assert.rejects(client.write({}, chunks()));
					assert.deepEqual(bodies, [Buffer.from("héllo")]);
				} else {
					assert.equal(await client.write(value), id);
					assert.deepEqual(bodies, [
						Buffer.from("héllo"),
						Buffer.from("héllo"),
					]);
				}
			} finally {
				client.close();
				for (const session of sessions) session.destroy();
				await new Promise<void>((resolve) => server.close(() => resolve()));
			}
		}
	} finally {
		tg.encoding.utf8 = utf8;
	}
});
