import * as tg from "../../../index.ts";
import { Body, Request, Response, Uri, percentEncode } from "../../../http.ts";
import type { Client } from "../../../client.ts";

type Connection = {
	input: Channel<tg.Process.Stdio.Read.ClientMessage>;
	output: AsyncIterableIterator<tg.Process.Stdio.Read.ServerMessage>;
};

class ProtocolError extends Error {}

export async function tryReadProcessStdio(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Read.Arg,
): Promise<AsyncIterableIterator<tg.Process.Stdio.Chunk> | null> {
	let connection = await connect(client, id, arg);
	if (connection === null) {
		return null;
	}

	return readProcessStdioAll(client, id, arg, connection);
}

async function* readProcessStdioAll(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Read.Arg,
	connection: Connection,
): AsyncIterableIterator<tg.Process.Stdio.Chunk> {
	let combined = arg.streams.length > 1;
	let forward =
		arg.length === undefined || arg.length === null || arg.length >= 0;
	let nextArg = { ...arg, streams: [...arg.streams] };
	let pendingNotification = false;
	let position = typeof arg.position === "number" ? arg.position : null;
	try {
		while (true) {
			if (pendingNotification && position !== null) {
				let message: tg.Process.Stdio.Read.ClientMessage = {
					kind: "notification",
					value: { kind: "read", value: { position } },
				};
				if (!connection.input.push(message)) {
					connection = await reconnect(client, id, nextArg, connection);
					continue;
				}
				pendingNotification = false;
			}
			let result: IteratorResult<tg.Process.Stdio.Read.ServerMessage>;
			try {
				result = await connection.output.next();
			} catch (error) {
				if (isTerminalError(error)) {
					throw error;
				}
				connection = await reconnect(client, id, nextArg, connection);
				continue;
			}
			if (result.done) {
				connection = await reconnect(client, id, nextArg, connection);
				continue;
			}
			let message = result.value;
			if (message.kind === "request") {
				if (message.value.kind !== "end") {
					throw new ProtocolError("invalid process stdio read request");
				}
				connection.input.push({
					kind: "response",
					value: { kind: "end" },
				});
				connection.input.close();
				return;
			}
			if (message.value.kind === "stop") {
				connection = await reconnect(client, id, nextArg, connection);
				continue;
			}
			if (message.value.kind !== "chunk") {
				throw new ProtocolError("invalid process stdio read notification");
			}
			let chunk = message.value.value;
			if (!arg.streams.includes(chunk.stream)) {
				throw new ProtocolError("invalid process stdio stream");
			}
			let start = combined ? chunk.combinedPosition : chunk.streamPosition;
			let end = start + chunk.bytes.length;
			if (!Number.isSafeInteger(end)) {
				throw new ProtocolError("the stdio position is too large");
			}
			if (position !== null) {
				if ((forward && end <= position) || (!forward && start >= position)) {
					pendingNotification = true;
					continue;
				}
				if ((forward && start > position) || (!forward && end < position)) {
					throw new ProtocolError("encountered a gap in the stdio stream");
				}
				if (forward && start < position) {
					let overlap = position - start;
					chunk = {
						...chunk,
						bytes: chunk.bytes.subarray(overlap),
						combinedPosition: chunk.combinedPosition + overlap,
						streamPosition: chunk.streamPosition + overlap,
					};
				} else if (!forward && end > position) {
					chunk = {
						...chunk,
						bytes: chunk.bytes.subarray(0, position - start),
					};
				}
			}
			let length = chunk.bytes.length;
			position = forward
				? (combined ? chunk.combinedPosition : chunk.streamPosition) + length
				: combined
					? chunk.combinedPosition
					: chunk.streamPosition;
			if (nextArg.length !== undefined && nextArg.length !== null) {
				if (nextArg.length >= 0) {
					nextArg.length -= Math.min(length, nextArg.length);
				} else {
					nextArg.length += Math.min(length, Math.abs(nextArg.length));
				}
			}
			nextArg.position = position;
			pendingNotification = true;

			yield chunk;
		}
	} finally {
		connection.input.close();
		await connection.output.return?.();
	}
}

async function connect(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Read.Arg,
): Promise<Connection | null> {
	let attempt = 0;
	while (true) {
		try {
			return await readProcessStdioOnce(client, id, arg);
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
	arg: tg.Process.Stdio.Read.Arg,
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

async function readProcessStdioOnce(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Read.Arg,
): Promise<Connection | null> {
	let input = new Channel<tg.Process.Stdio.Read.ClientMessage>();
	let uri = new Uri({
		path: `/processes/${percentEncode(id)}/stdio/read`,
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
	input: AsyncIterable<tg.Process.Stdio.Read.ClientMessage>,
): AsyncIterableIterator<Body.SseEvent> {
	for await (let message of input) {
		yield {
			data: JSON.stringify(message.value),
			event: message.kind,
		};
	}
}

async function* decodeServerMessages(
	response: Response,
): AsyncIterableIterator<tg.Process.Stdio.Read.ServerMessage> {
	for await (let event of response.sse()) {
		try {
			if (event.event === "error") {
				throw errorFromData(
					JSON.parse(event.data) as tg.Error.Data | tg.Error.Id,
				);
			}
			if (event.event === "notification") {
				let value = JSON.parse(event.data) as
					| { kind: "chunk"; value: tg.Process.Stdio.Chunk.Data }
					| { kind: "stop" };
				if (value.kind === "chunk") {
					yield {
						kind: "notification",
						value: {
							kind: "chunk",
							value: tg.Process.Stdio.Chunk.fromData(value.value),
						},
					};
				} else if (value.kind === "stop") {
					yield { kind: "notification", value };
				} else {
					throw new ProtocolError("invalid process stdio read notification");
				}
			} else if (event.event === "request") {
				let value = JSON.parse(
					event.data,
				) as tg.Process.Stdio.Read.ServerRequest;
				if (value.kind !== "end") {
					throw new ProtocolError("invalid process stdio read request");
				}
				yield { kind: "request", value };
			} else {
				throw new ProtocolError("invalid process stdio read message");
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
