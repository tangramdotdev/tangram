import * as tg from "../../../index.ts";
import { Request, Response, Uri, percentEncode } from "../../../http.ts";
import type { Client } from "../../../client.ts";

export namespace Stdio {
	export namespace Read {
		export type Arg = {
			length?: number | undefined;
			location?: tg.Location.Arg | undefined;
			position?: number | string | undefined;
			size?: number | undefined;
			streams: Array<tg.Process.Stdio.Stream>;
			timeout?: number | undefined;
		};
	}
}

export async function tryReadProcessStdio(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Read.Arg,
): Promise<AsyncIterableIterator<tg.Process.Stdio.Read.Event> | undefined> {
	let stream = await readProcessStdioOnce(client, id, arg);
	if (stream === undefined) {
		return undefined;
	}
	return readProcessStdioAll(client, id, arg, stream);
}

async function* readProcessStdioAll(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Read.Arg,
	stream: AsyncIterableIterator<tg.Process.Stdio.Read.Event>,
): AsyncIterableIterator<tg.Process.Stdio.Read.Event> {
	let nextArg: tg.Process.Stdio.Read.Arg = {
		...arg,
		streams: [...arg.streams],
	};
	let current: AsyncIterableIterator<tg.Process.Stdio.Read.Event> | undefined =
		stream;
	try {
		while (true) {
			if (current === undefined) {
				current = await readProcessStdioOnce(client, id, nextArg);
				if (current === undefined) {
					throw new Error("failed to find the process");
				}
			}
			let result = await current.next();
			if (result.done) {
				current = undefined;
				continue;
			}
			let event = result.value;
			if (event.kind === "end") {
				yield event;
				break;
			}
			updateStdioReadArg(nextArg, event.value);
			yield event;
		}
	} finally {
		await current?.return?.();
	}
}

async function readProcessStdioOnce(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Read.Arg,
): Promise<AsyncIterableIterator<tg.Process.Stdio.Read.Event> | undefined> {
	let method = "GET";
	let uri = new Uri({
		path: `/processes/${percentEncode(id)}/stdio`,
		query: {
			...arg,
			location:
				arg.location === undefined
					? undefined
					: tg.Location.Arg.toDataString(arg.location),
			streams: arg.streams.join(","),
		},
	});
	let headers = {
		accept: "text/event-stream",
	};
	let request = new Request({
		method,
		uri,
		headers,
	});
	let response = await client.send(request);
	if (response.status === 404) {
		return undefined;
	} else if (response.status < 200 || response.status >= 300) {
		let error: unknown;
		try {
			error = tg.Error.fromData(await response.json<tg.Error.Data>());
		} catch {
			error = new Error("the request failed");
		}
		throw error;
	}
	return decodeReadStdioEvents(response);
}

async function* decodeReadStdioEvents(
	response: Response,
): AsyncIterableIterator<tg.Process.Stdio.Read.Event> {
	for await (let event of response.sse()) {
		if (event.event === undefined) {
			let value = JSON.parse(
				event.data,
			) as tg.Process.Stdio.Read.Event.Data.Chunk;
			yield tg.Process.Stdio.Read.Event.fromData({ kind: "chunk", value });
		} else if (event.event === "end") {
			yield { kind: "end" };
			break;
		} else if (event.event === "error") {
			let data = JSON.parse(event.data) as tg.Error.Data | tg.Error.Id;
			if (typeof data === "string") {
				throw tg.Error.withId(data);
			} else {
				throw tg.Error.fromData(data);
			}
		} else {
			throw new Error("invalid process stdio event");
		}
	}
}

function updateStdioReadArg(
	arg: tg.Process.Stdio.Read.Arg,
	chunk: tg.Process.Stdio.Chunk,
) {
	if (chunk.position === undefined) {
		return;
	}
	let length = chunk.bytes.length;
	let forward = arg.length === undefined || arg.length >= 0;
	if (arg.length !== undefined) {
		if (arg.length >= 0) {
			arg.length -= Math.min(length, arg.length);
		} else {
			arg.length += Math.min(length, Math.abs(arg.length));
		}
	}
	if (forward) {
		arg.position = chunk.position + length;
	} else {
		arg.position = Math.max(0, chunk.position - 1);
	}
}
