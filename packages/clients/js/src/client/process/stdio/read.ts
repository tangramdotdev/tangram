import * as tg from "../../../index.ts";
import { Body, Request, Response, Uri, percentEncode } from "../../../http.ts";
import { Receiver } from "../../../process/stdio/flow.ts";
import type { Client } from "../../../client.ts";

export type Connection = {
	input: {
		close(): void;
		push(message: tg.Process.Stdio.Read.ClientMessage): boolean;
	};
	output: AsyncIterableIterator<tg.Process.Stdio.Read.ServerMessage>;
	reconnect?: (arg: tg.Process.Stdio.Read.Arg) => Promise<Connection>;
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

export function readProcessStdioAll(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Read.Arg,
	connection: Connection,
): AsyncIterableIterator<tg.Process.Stdio.Chunk> {
	let state = { canceled: false, connection };
	let output = readProcessStdioAllInner(client, id, arg, state);
	return {
		next: () => output.next(),
		return: async () => {
			state.canceled = true;
			state.connection.input.close();
			state.connection.output.return?.().catch(() => {});
			return await output.return!();
		},
		[Symbol.asyncIterator]() {
			return this;
		},
	};
}

async function* readProcessStdioAllInner(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Read.Arg,
	state: { canceled: boolean; connection: Connection },
): AsyncIterableIterator<tg.Process.Stdio.Chunk> {
	let connection = state.connection;
	let combined = arg.streams.length > 1;
	let forward =
		arg.length === undefined || arg.length === null || arg.length >= 0;
	let nextArg = { ...arg, streams: [...arg.streams] };
	let window = new Receiver();
	let pending = 0;
	let position = typeof arg.position === "string" ? null : (arg.position ?? 0);
	try {
		while (!state.canceled) {
			let progress = window.consume(pending);
			pending = 0;
			if (progress !== null) {
				connection.input.push({ kind: "notification", value: progress });
			}
			let result: IteratorResult<tg.Process.Stdio.Read.ServerMessage> | null =
				null;
			try {
				result = await connection.output.next();
			} catch (error) {
				if (state.canceled) return;
				if (isTerminalError(error)) {
					throw error;
				}
			}
			if (state.canceled) return;
			if (result === null || result.done) {
				connection = state.connection = await reconnect(
					client,
					id,
					nextArg,
					connection,
				);
				window = new Receiver();
				continue;
			}
			let message = result.value;
			if (message.kind === "response") {
				tg.Process.Stdio.Read.Output.validate(
					message.value,
					arg.streams,
					position ?? 0,
				);
				connection.input.push({ kind: "ack" });
				connection.input.close();
				return;
			}
			if (message.value.kind === "position") {
				let value = message.value.value;
				if (
					!Number.isSafeInteger(value.position) ||
					value.position < 0 ||
					(value.length !== null && !Number.isSafeInteger(value.length))
				) {
					throw new ProtocolError("invalid process stdio position");
				}
				position = value.position;
				nextArg.position = position;
				nextArg.length = value.length;
				continue;
			}
			if (message.value.kind !== "chunk") {
				throw new ProtocolError("invalid process stdio read notification");
			}
			let chunk = message.value.value;
			pending = chunk.bytes.length;
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
	let next =
		connection.reconnect === undefined
			? await connect(client, id, arg)
			: await connection.reconnect(arg);
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
			data: JSON.stringify(message.kind === "ack" ? null : message.value),
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
					| {
							kind: "position";
							value: { length: number | null; position: number };
					  };
				if (value.kind === "chunk") {
					yield {
						kind: "notification",
						value: {
							kind: "chunk",
							value: tg.Process.Stdio.Chunk.fromData(value.value),
						},
					};
				} else if (value.kind === "position") {
					yield { kind: "notification", value };
				} else {
					throw new ProtocolError("invalid process stdio read notification");
				}
			} else if (event.event === "response") {
				let value = tg.Process.Stdio.Read.Output.fromData(
					JSON.parse(event.data),
				);
				if (!["end", "limit", "timeout"].includes(value.kind)) {
					throw new ProtocolError("invalid process stdio read request");
				}
				yield { kind: "response", value };
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
