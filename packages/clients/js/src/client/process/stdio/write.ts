import * as tg from "../../../index.ts";
import { Body, Request, Response, Uri, percentEncode } from "../../../http.ts";
import { Queue } from "../../../queue.ts";
import { Stop } from "../../../stop.ts";
import type { Client } from "../../../client.ts";

export namespace Stdio {
	export namespace Write {
		export type Arg = {
			location?: tg.Location.Arg | null;
			streams: Array<tg.Process.Stdio.Stream>;
			token?: tg.Grant.Token | null;
		};
	}
}

export async function writeProcessStdio(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Write.Arg,
	input: AsyncIterableIterator<tg.Process.Stdio.Read.Event>,
): Promise<AsyncIterableIterator<tg.Process.Stdio.Write.Event>> {
	let output = await tryWriteProcessStdio(client, id, arg, input);
	if (output === null) {
		throw new Error("failed to find the process");
	}
	return output;
}

export async function tryWriteProcessStdio(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Write.Arg,
	input: AsyncIterableIterator<tg.Process.Stdio.Read.Event>,
): Promise<AsyncIterableIterator<tg.Process.Stdio.Write.Event> | null> {
	return await writeProcessStdioAll(client, id, arg, input);
}

async function writeProcessStdioAll(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Write.Arg,
	input: AsyncIterableIterator<tg.Process.Stdio.Read.Event>,
): Promise<AsyncIterableIterator<tg.Process.Stdio.Write.Event> | null> {
	let events = new Queue(input);
	let stop = new Stop();
	let output = await writeProcessStdioOnce(
		client,
		id,
		arg,
		encodeReadStdioEvents(events, arg.streams, stop),
	);
	if (output === null) {
		await input.return?.();
		return null;
	}
	return (async function* (output_) {
		try {
			while (true) {
				let stopped = false;
				for await (let event of output_) {
					yield event;
					if (event.kind === "stop") {
						if (!stopped) {
							stopped = true;
							stop.stop();
						}
					} else if (event.kind === "end") {
						return;
					}
				}
				if (!stopped) {
					return;
				}
				stop = new Stop();
				let nextOutput = await writeProcessStdioOnce(
					client,
					id,
					arg,
					encodeReadStdioEvents(events, arg.streams, stop),
				);
				if (nextOutput === null) {
					return;
				}
				output_ = nextOutput;
			}
		} finally {
			await input.return?.();
		}
	})(output);
}

async function writeProcessStdioOnce(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Write.Arg,
	input: AsyncIterableIterator<Body.SseEvent>,
): Promise<AsyncIterableIterator<tg.Process.Stdio.Write.Event> | null> {
	let method = "POST";
	let uri = new Uri({
		path: `/processes/${percentEncode(id)}/stdio`,
		query: {
			...arg,
			location:
				arg.location === undefined || arg.location === null
					? null
					: tg.Location.Arg.toDataString(arg.location),
			streams: arg.streams.join(","),
		},
	});
	let headers = {
		accept: "text/event-stream",
		"content-type": "text/event-stream",
	};
	let body = Body.sse(input);
	let request = new Request({
		body,
		method,
		uri,
		headers,
	});
	let response = await client.send(request);
	if (response.status === 404) {
		return null;
	} else if (response.status < 200 || response.status >= 300) {
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	return decodeWriteStdioEvents(response);
}

async function* decodeWriteStdioEvents(
	response: Response,
): AsyncIterableIterator<tg.Process.Stdio.Write.Event> {
	for await (let event of response.sse()) {
		if (event.event === "error") {
			let data = JSON.parse(event.data) as tg.Error.Data | tg.Error.Id;
			if (typeof data === "string") {
				throw tg.Error.withId(data);
			} else {
				throw tg.Error.fromData(data);
			}
		} else if (event.event === "end") {
			yield { kind: "end" };
			break;
		} else if (event.event === "stop") {
			yield { kind: "stop" };
		} else if (event.event === "write") {
			continue;
		} else {
			throw new Error("invalid process stdio event");
		}
	}
}

async function* encodeReadStdioEvents(
	input: Queue<tg.Process.Stdio.Read.Event>,
	streams: Array<tg.Process.Stdio.Stream>,
	stop: Stop,
): AsyncIterableIterator<Body.SseEvent> {
	let ended = false;
	while (true) {
		let result = await input.next(stop.promise);
		if (result.done) {
			break;
		}
		let event = result.value;
		let data = tg.Process.Stdio.Read.Event.toData(event);
		if (data.kind === "end") {
			ended = true;
			yield {
				data: "",
				event: "end",
			};
			break;
		}
		if (!streams.includes(data.value.stream)) {
			throw new Error("invalid process stdio stream");
		}
		yield {
			data: JSON.stringify(data.value),
		};
	}
	if (!ended) {
		yield {
			data: "",
			event: "end",
		};
	}
}
