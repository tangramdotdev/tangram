import * as tg from "../../../index.ts";
import { Body, Request, Response, Uri, percentEncode } from "../../../http.ts";
import { chunkSize, maxChunks } from "../../../process/stdio/flow.ts";
import type { Client } from "../../../client.ts";

export type Connection = {
	input: {
		close(): void;
		push(message: tg.Process.Stdio.Write.ClientMessage): boolean;
	};
	output: AsyncIterableIterator<tg.Process.Stdio.Write.ServerMessage>;
	reconnect?: () => Promise<Connection>;
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
	arg: tg.Process.Stdio.Write.Stream.Arg,
	input: AsyncIterableIterator<tg.Process.Stdio.Chunk>,
	complete?: (chunk: tg.Process.Stdio.Chunk) => void,
): Promise<void> {
	let output = await tryWriteProcessStdio(client, id, arg, input, complete);
	if (output === null) {
		throw new Error("failed to find the process");
	}
}

export async function tryWriteProcessStdio(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Write.Stream.Arg,
	input: AsyncIterableIterator<tg.Process.Stdio.Chunk>,
	complete?: (chunk: tg.Process.Stdio.Chunk) => void,
): Promise<true | null> {
	let connection = await connect(client, id, arg);
	if (connection === null) {
		await input.return?.();
		return null;
	}
	await writeProcessStdioAll(client, id, arg, input, connection, complete);

	return true;
}

export async function writeProcessStdioAll(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Write.Stream.Arg,
	input: AsyncIterableIterator<tg.Process.Stdio.Chunk>,
	connection: Connection,
	complete?: (chunk: tg.Process.Stdio.Chunk) => void,
): Promise<void> {
	type Pending = {
		original?: tg.Process.Stdio.Chunk;
		request: tg.Process.Stdio.Write.Request;
		sent: boolean;
	};
	let pending: Array<Pending> = [];
	let remaining: { chunk: tg.Process.Stdio.Chunk; offset: number } | null =
		null;
	let inputEvent: Promise<WriteEvent> | null = null;
	let outputEvent: Promise<WriteEvent> | null = null;
	let inputEnded = false;
	let nextId = 0;
	let combinedPosition = 0;
	let streamPositions: Partial<Record<tg.Process.Stdio.Stream, number>> =
		Object.fromEntries(arg.streams.map((stream) => [stream, 0]));
	try {
		while (true) {
			while (remaining !== null && pending.length < maxChunks) {
				let { chunk, offset } = remaining;
				if (!arg.streams.includes(chunk.stream)) {
					throw new ProtocolError("invalid process stdio stream");
				}
				let length = Math.min(chunkSize, chunk.bytes.length - offset);
				if (length === 0) {
					complete?.(chunk);
					remaining = null;
					break;
				}
				let value = {
					...chunk,
					bytes: chunk.bytes.subarray(offset, offset + length),
					combinedPosition: chunk.combinedPosition + offset,
					streamPosition: chunk.streamPosition + offset,
				};
				combinedPosition = value.combinedPosition + length;
				streamPositions[value.stream] = value.streamPosition + length;
				if (
					!Number.isSafeInteger(combinedPosition) ||
					!Number.isSafeInteger(streamPositions[value.stream])
				) {
					throw new ProtocolError("invalid stdio position");
				}
				remaining.offset += length;
				let last = remaining.offset === chunk.bytes.length;
				pending.push({
					...(last ? { original: chunk } : {}),
					request: { arg: { kind: "chunk", value }, id: nextId++ },
					sent: false,
				});
				if (last) {
					remaining = null;
				}
			}
			if (inputEnded && pending.length === 0) {
				pending.push({
					request: {
						arg: { kind: "end", value: { combinedPosition, streamPositions } },
						id: nextId++,
					},
					sent: false,
				});
			}
			for (let value of pending) {
				if (!value.sent) {
					connection.input.push({ kind: "request", value: value.request });
					value.sent = true;
				}
			}
			if (
				pending.length < maxChunks &&
				remaining === null &&
				!inputEnded &&
				inputEvent === null
			) {
				inputEvent = nextInput(input);
			}
			outputEvent ??= nextOutput(connection.output);
			let event =
				inputEvent === null
					? await outputEvent
					: await Promise.race([outputEvent, inputEvent]);
			if (event.kind === "input_error") {
				throw event.error;
			}
			if (event.kind === "input") {
				inputEvent = null;
				if (event.result.done) {
					inputEnded = true;
				} else {
					remaining = { chunk: event.result.value, offset: 0 };
				}
				continue;
			}
			outputEvent = null;
			if (event.kind === "output_error" && isTerminalError(event.error)) {
				throw event.error;
			}
			if (event.kind === "output_error" || event.result.done) {
				connection = await reconnect(client, id, arg, connection);
				for (let value of pending) {
					value.sent = false;
				}
				continue;
			}
			let message = event.result.value;
			if (message.kind === "ack") {
				continue;
			}
			let response = message.value;
			connection.input.push({ kind: "ack", value: { id: response.id } });
			let value = pending.shift();
			if (value === undefined || value.request.id !== response.id) {
				throw new ProtocolError(
					"received an out-of-order stdio write response",
				);
			}
			if (response.error !== null) {
				throw tg.Error.fromData(response.error);
			}
			if (response.output === null) {
				throw new ProtocolError("missing the stdio write output");
			}
			let { closed, length } = response.output;
			let expected =
				value.request.arg.kind === "chunk"
					? value.request.arg.value.bytes.length
					: 0;
			if (
				!Number.isSafeInteger(length) ||
				length < 0 ||
				length > expected ||
				(!closed && length !== expected)
			) {
				throw new ProtocolError("invalid process stdio write length");
			}
			if (length === expected && value.original !== undefined) {
				complete?.(value.original);
			}
			if (value.request.arg.kind === "end" && !closed) {
				throw new ProtocolError("the stdio end was not confirmed");
			}
			if (closed || value.request.arg.kind === "end") {
				return;
			}
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
	arg: tg.Process.Stdio.Write.Stream.Arg,
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
	arg: tg.Process.Stdio.Write.Stream.Arg,
	connection: Connection,
): Promise<Connection> {
	connection.input.close();
	await connection.output.return?.();
	let next =
		connection.reconnect === undefined
			? await connect(client, id, arg)
			: await connection.reconnect();
	if (next === null) {
		throw new Error("failed to find the process");
	}

	return next;
}

async function writeProcessStdioOnce(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Write.Stream.Arg,
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
			message.kind === "request"
				? {
						...message.value,
						arg: tg.Process.Stdio.Write.Data.toData(message.value.arg),
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
			if (event.event !== "ack" && event.event !== "response") {
				throw new ProtocolError("invalid process stdio write message");
			}
			yield {
				kind: event.event,
				value: JSON.parse(event.data),
			} as tg.Process.Stdio.Write.ServerMessage;
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
