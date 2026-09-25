import { strict as assert } from "node:assert";
import { setImmediate } from "node:timers/promises";
import { test } from "node:test";
import { requestWindow } from "../src/client/process/connect.ts";
import * as tg from "../src/index.ts";
import { Body } from "../src/http.ts";
import { Channel } from "../src/process/connect/channel.ts";
import { Session } from "../src/process/connect/session.ts";

test("reads and closes share the fixed receipt window", async () => {
	const send = tg.client.send;
	const utf8 = tg.encoding.utf8;
	tg.encoding.utf8 = {
		decode: (value) => new TextDecoder().decode(value),
		encode: (value) => new TextEncoder().encode(value),
	};
	const events = new Channel<{ event: string; data: string }>(16);
	const emit = (event: string, value: unknown) =>
		events.push({ event, data: JSON.stringify(value) });
	let session: Session | undefined;
	try {
		tg.client.send = async () =>
			new tg.Response(
				200,
				{ "content-type": "text/event-stream" },
				Body.sse(events),
			);
		emit("response", {
			id: 0,
			error: null,
			output: {
				kind: "connect",
				value: {
					cached: false,
					lease: null,
					location: null,
					process: "pcs_010000000000000000000000000000000000000000000000000000",
					tokens: {},
					wait: null,
				},
			},
		});
		({ connection: session } = await Session.open({
			mode: "run",
			process: "pcs_010000000000000000000000000000000000000000000000000000",
			reads: {},
		}));
		for (let i = 0; i < requestWindow / 2; i++) {
			session.read({ streams: ["stdout"] }).input.close();
		}
		assert.throws(
			() => session!.read({ streams: ["stdout"] }),
			/request window was exceeded/,
		);
		emit("ack", { id: 1 });
		await setImmediate();
		session.read({ streams: ["stdout"] });

		// Detachment retains one reserved request even when ordinary credit is exhausted.
		const detaching = session.detach();
		const detachId = requestWindow + 3;
		emit("ack", { id: detachId });
		emit("response", { id: detachId, error: null, output: { kind: "detach" } });
		await detaching;
	} finally {
		session?.close();
		events.close();
		tg.client.send = send;
		tg.encoding.utf8 = utf8;
	}
});
