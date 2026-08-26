import * as tg from "../../../index.ts";
import { Body, Request, Response, Uri, percentEncode } from "../../../http.ts";
import type { Client } from "../../../client.ts";

type Connection = {
	input: Channel<tg.Process.Stdio.Write.ClientMessage>;
	output: AsyncIterableIterator<tg.Process.Stdio.Write.ServerMessage>;
};

type WriteEvent =
	| { kind: "input"; result: IteratorResult<tg.Process.Stdio.Chunk> }
	| { error: unknown; kind: "input_error" }
	| {
			kind: "output";
			result: IteratorResult<tg.Process.Stdio.Write.ServerMessage>;
	  }
	| { error: unknown; kind: "output_error" };

class ProtocolError extends Error {}

export async function writeProcessStdio(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Write.Arg,
	input: AsyncIterableIterator<tg.Process.Stdio.Chunk>,
): Promise<void> {
	let output = await tryWriteProcessStdio(client, id, arg, input);
	if (output === null) {
		throw new Error("failed to find the process");
	}
}

export async function tryWriteProcessStdio(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Write.Arg,
	input: AsyncIterableIterator<tg.Process.Stdio.Chunk>,
): Promise<true | null> {
	let connection = await connect(client, id, arg);
	if (connection === null) {
		await input.return?.();
		return null;
	}
	await writeProcessStdioAll(client, id, arg, input, connection);

	return true;
}

async function writeProcessStdioAll(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Write.Arg,
	input: AsyncIterableIterator<tg.Process.Stdio.Chunk>,
	connection: Connection,
): Promise<void> {
	let combined = arg.streams.length > 1;
	let endSent = false;
	let inputEvent: Promise<WriteEvent> | null = null;
	let inputEnded = false;
	let outputEvent: Promise<WriteEvent> | null = null;
	let pending: tg.Process.Stdio.Chunk | null = null;
	let pendingSent = false;
	try {
		while (true) {
			if (pending === null && !inputEnded && inputEvent === null) {
				inputEvent = nextInput(input);
			}
			outputEvent ??= nextOutput(connection.output);
			if (pending !== null && !pendingSent) {
				let message: tg.Process.Stdio.Write.ClientMessage = {
					kind: "notification",
					value: { kind: "chunk", value: pending },
				};
				if (!connection.input.push(message)) {
					outputEvent = null;
					connection = await reconnect(client, id, arg, connection);
					endSent = false;
					continue;
				}
				pendingSent = true;
			} else if (pending === null && inputEnded && !endSent) {
				let message: tg.Process.Stdio.Write.ClientMessage = {
					kind: "request",
					value: { kind: "end" },
				};
				if (!connection.input.push(message)) {
					outputEvent = null;
					connection = await reconnect(client, id, arg, connection);
					continue;
				}
				endSent = true;
			}
			let event =
				inputEvent === null
					? await outputEvent
					: await Promise.race([inputEvent, outputEvent]);
			if (event.kind === "input_error") {
				throw event.error;
			}
			if (event.kind === "input") {
				inputEvent = null;
				if (event.result.done) {
					inputEnded = true;
				} else {
					pending = event.result.value;
					if (!arg.streams.includes(pending.stream)) {
						throw new ProtocolError("invalid process stdio stream");
					}
				}
				continue;
			}
			outputEvent = null;
			if (event.kind === "output_error") {
				let error = event.error;
				if (isTerminalError(error)) {
					throw error;
				}
				connection = await reconnect(client, id, arg, connection);
				endSent = false;
				pendingSent = false;
				continue;
			}
			let result = event.result;
			if (result.done) {
				connection = await reconnect(client, id, arg, connection);
				endSent = false;
				pendingSent = false;
				continue;
			}
			let message = result.value;
			if (message.kind === "response") {
				if (message.value.kind !== "end") {
					throw new ProtocolError("invalid process stdio write response");
				}
				return;
			}
			if (message.value.kind === "stop") {
				connection = await reconnect(client, id, arg, connection);
				endSent = false;
				pendingSent = false;
				continue;
			}
			if (message.value.kind !== "write") {
				throw new ProtocolError("invalid process stdio write notification");
			}
			if (pending === null) {
				continue;
			}
			let position = message.value.value.position;
			let start = combined ? pending.combinedPosition : pending.streamPosition;
			let end = start + pending.bytes.length;
			if (!Number.isSafeInteger(position) || position < 0 || position > end) {
				throw new ProtocolError("invalid process stdio write position");
			}
			if (position <= start) {
				pendingSent = false;
				continue;
			}
			if (position < end) {
				let overlap = position - start;
				pending = {
					...pending,
					bytes: pending.bytes.subarray(overlap),
					combinedPosition: pending.combinedPosition + overlap,
					streamPosition: pending.streamPosition + overlap,
				};
			} else {
				pending = null;
			}
			pendingSent = false;
		}
	} finally {
		connection.input.close();
		await connection.output.return?.();
		input.return?.().catch(() => {});
	}
}

function nextInput(
	input: AsyncIterableIterator<tg.Process.Stdio.Chunk>,
): Promise<WriteEvent> {
	return input.next().then(
		(result) => ({ kind: "input", result }),
		(error: unknown) => ({ error, kind: "input_error" }),
	);
}

function nextOutput(
	output: AsyncIterableIterator<tg.Process.Stdio.Write.ServerMessage>,
): Promise<WriteEvent> {
	return output.next().then(
		(result) => ({ kind: "output", result }),
		(error: unknown) => ({ error, kind: "output_error" }),
	);
}

async function connect(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Write.Arg,
): Promise<Connection | null> {
	let attempt = 0;
	while (true) {
		try {
			return await writeProcessStdioOnce(client, id, arg);
		} catch (error) {
			if (isTerminalError(error)) {
				throw error;
			}
			await retryDelay(attempt);
			attempt++;
		}
	}
}

async function reconnect(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Write.Arg,
	connection: Connection,
): Promise<Connection> {
	connection.input.close();
	await connection.output.return?.();
	let next = await connect(client, id, arg);
	if (next === null) {
		throw new Error("failed to find the process");
	}

	return next;
}

async function writeProcessStdioOnce(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Write.Arg,
): Promise<Connection | null> {
	let input = new Channel<tg.Process.Stdio.Write.ClientMessage>();
	let uri = new Uri({
		path: `/processes/${percentEncode(id)}/stdio/write`,
		query: {
			...arg,
			location:
				arg.location === undefined || arg.location === null
					? null
					: tg.Location.Arg.toDataString(arg.location),
			streams: arg.streams.join(","),
		},
	});
	let request = new Request({
		body: Body.sse(encodeClientMessages(input)),
		headers: {
			accept: "text/event-stream",
			"content-type": "text/event-stream",
		},
		method: "POST",
		uri,
	});
	let response = await client.send(request);
	if (response.status === 404) {
		input.close();
		return null;
	}
	if (response.status < 200 || response.status >= 300) {
		input.close();
		throw await responseError(response);
	}
	let contentType = response.headers.get("content-type")?.split(";", 1)[0];
	if (contentType !== "text/event-stream") {
		input.close();
		throw new ProtocolError("invalid process stdio response content type");
	}
	let output = decodeServerMessages(response);

	return { input, output };
}

async function* encodeClientMessages(
	input: AsyncIterable<tg.Process.Stdio.Write.ClientMessage>,
): AsyncIterableIterator<Body.SseEvent> {
	for await (let message of input) {
		let value =
			message.kind === "notification" && message.value.kind === "chunk"
				? {
						...message.value,
						value: tg.Process.Stdio.Chunk.toData(message.value.value),
					}
				: message.value;
		yield {
			data: JSON.stringify(value),
			event: message.kind,
		};
	}
}

async function* decodeServerMessages(
	response: Response,
): AsyncIterableIterator<tg.Process.Stdio.Write.ServerMessage> {
	for await (let event of response.sse()) {
		try {
			if (event.event === "error") {
				throw errorFromData(
					JSON.parse(event.data) as tg.Error.Data | tg.Error.Id,
				);
			}
			if (event.event === "notification") {
				let value = JSON.parse(
					event.data,
				) as tg.Process.Stdio.Write.ServerNotification;
				if (value.kind !== "stop" && value.kind !== "write") {
					throw new ProtocolError("invalid process stdio write notification");
				}
				yield { kind: "notification", value };
			} else if (event.event === "response") {
				let value = JSON.parse(
					event.data,
				) as tg.Process.Stdio.Write.ServerResponse;
				if (value.kind !== "end") {
					throw new ProtocolError("invalid process stdio write response");
				}
				yield { kind: "response", value };
			} else {
				throw new ProtocolError("invalid process stdio write message");
			}
		} catch (error) {
			if (error instanceof tg.Error || error instanceof ProtocolError) {
				throw error;
			}
			throw new ProtocolError("failed to deserialize a process stdio message", {
				cause: error,
			});
		}
	}
}

async function responseError(response: Response): Promise<tg.Error> {
	try {
		return errorFromData(await response.json<tg.Error.Data | tg.Error.Id>());
	} catch (error) {
		if (error instanceof tg.Error) {
			throw error;
		}
		throw new ProtocolError("failed to deserialize the error response", {
			cause: error,
		});
	}
}

function errorFromData(data: tg.Error.Data | tg.Error.Id): tg.Error {
	return typeof data === "string"
		? tg.Error.withId(data)
		: tg.Error.fromData(data);
}

function isTerminalError(error: unknown): boolean {
	return error instanceof tg.Error || error instanceof ProtocolError;
}

async function retryDelay(attempt: number): Promise<void> {
	let delay = Math.min(0.01 * 2 ** Math.min(attempt, 7), 1);
	await tg.sleep(delay);
}

class Channel<T> implements AsyncIterableIterator<T> {
	#closed = false;
	#values: Array<T> = [];
	#waiters: Array<(result: IteratorResult<T>) => void> = [];

	close(): void {
		if (this.#closed) {
			return;
		}
		this.#closed = true;
		while (this.#waiters.length > 0) {
			this.#waiters.shift()!({ done: true, value: undefined });
		}
	}

	next(): Promise<IteratorResult<T>> {
		let value = this.#values.shift();
		if (value !== undefined) {
			return Promise.resolve({ done: false, value });
		}
		if (this.#closed) {
			return Promise.resolve({ done: true, value: undefined });
		}
		return new Promise((resolve) => this.#waiters.push(resolve));
	}

	push(value: T): boolean {
		if (this.#closed) {
			return false;
		}
		let waiter = this.#waiters.shift();
		if (waiter === undefined) {
			this.#values.push(value);
		} else {
			waiter({ done: false, value });
		}
		return true;
	}

	return(): Promise<IteratorResult<T>> {
		this.close();
		return Promise.resolve({ done: true, value: undefined });
	}

	[Symbol.asyncIterator](): AsyncIterableIterator<T> {
		return this;
	}
}
