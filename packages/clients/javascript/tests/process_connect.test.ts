import { strict as assert } from "node:assert";
import { setImmediate } from "node:timers/promises";
import { test } from "node:test";
import * as tg from "../src/index.ts";
import { Body } from "../src/http.ts";
import { Channel } from "../src/process/connect/channel.ts";
import { Session } from "../src/process/connect/session.ts";

test("operations share one pending request limit", async () => {
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
				Body.sse(events).prepend(new Uint8Array([2, 123, 125])),
			);
		emit("notification", {
			kind: "progress",
			value: {
				kind: "output",
				value: {
					cached: false,
					lease: null,
					location: null,
					process: "pcs_010000000000000000000000000000000000000000000000000000",
					tokens: {},
					outcome: null,
				},
			},
		});
		({ connection: session } = await Session.open({
			mode: "run",
			process: "pcs_010000000000000000000000000000000000000000000000000000",
		}));
		const signals = [];
		for (let i = 0; i < 64; i++) {
			const signal = session.signal({ signal: "TERM" });
			signal.catch(() => {});
			signals.push(signal);
		}
		await assert.rejects(
			session.cancel({ lease: "lease" }),
			/too many pending/,
		);
		// Receipt acknowledgments neither complete operations nor free pending entries.
		emit("ack", { id: 1 });
		await setImmediate();
		await assert.rejects(
			session.signal({ signal: "TERM" }),
			/too many pending/,
		);
		// A response frees its pending entry.
		emit("response", { id: 1, error: null, output: { kind: "signal" } });
		await signals[0];
		const signal = session.signal({ signal: "TERM" });
		emit("response", { id: 67, error: null, output: { kind: "signal" } });
		await signal;
		const detaching = session.detach();
		emit("response", { id: 68, error: null, output: { kind: "detach" } });
		await detaching;
	} finally {
		session?.close();
		events.close();
		tg.client.send = send;
		tg.encoding.utf8 = utf8;
	}
});
