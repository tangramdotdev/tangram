import * as tg from "../../../index.ts";
import { Body, Request, Response, percentEncode } from "../../../http.ts";
import {
	type Client,
	backoff,
	isTransportError,
	retry,
} from "../../../client.ts";

export type Connection = {
	input: Channel<tg.Process.Stdio.Write.Data>;
	output: AsyncIterableIterator<tg.Process.Stdio.Write.Output>;
};

type WriteEvent =
	| { kind: "drained" }
	| { kind: "input"; result: IteratorResult<tg.Process.Stdio.Chunk> }
	| { error: unknown; kind: "input_error" }
	| {
			kind: "output";
			result: IteratorResult<tg.Process.Stdio.Write.Output>;
	  }
	| { error: unknown; kind: "output_error" };

class ProtocolError extends Error {}

class NotFoundError extends Error {}

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
	try {
		await writeProcessStdioAll(client, id, arg, input, connection, complete);
	} catch (error) {
		if (error instanceof NotFoundError) {
			return null;
		}
		throw error;
	}

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
	// Each pending write is held until the server reports a position at or past its end.
	type Pending = {
		data: tg.Process.Stdio.Write.Data;
		end: number;
		original?: tg.Process.Stdio.Chunk;
		sent: boolean;
	};
	let pending: Array<Pending> = [];
	let written = 0;
	let inputEvent: Promise<WriteEvent> | null = null;
	let outputEvent: Promise<WriteEvent> | null = null;
	let drainEvent: Promise<WriteEvent> | null = null;
	let inputEnded = false;
	let combinedPosition = 0;
	let streamPositions: Partial<Record<tg.Process.Stdio.Stream, number>> =
		Object.fromEntries(arg.streams.map((stream) => [stream, 0]));
	let reconnects = 0;
	try {
		while (true) {
			// Send while the request body is drained, so HTTP/2 backpressure bounds the pending writes.
			for (let value of pending) {
				if (value.sent) {
					continue;
				}
				if (connection.input.size > 0) {
					break;
				}
				let data = value.data;
				if (data.kind === "chunk" && data.value.combinedPosition < written) {
					let offset = written - data.value.combinedPosition;
					data = {
						kind: "chunk",
						value: {
							...data.value,
							bytes: data.value.bytes.subarray(offset),
							combinedPosition: data.value.combinedPosition + offset,
							streamPosition: data.value.streamPosition + offset,
						},
					};
				}
				connection.input.push(data);
				value.sent = true;
			}
			let unsent = pending.some((value) => !value.sent);
			if (
				!unsent &&
				!inputEnded &&
				inputEvent === null &&
				connection.input.size === 0
			) {
				inputEvent = nextInput(input);
			}
			if (connection.input.size > 0 && drainEvent === null) {
				drainEvent = connection.input
					.drained()
					.then(() => ({ kind: "drained" }) as WriteEvent);
			}
			outputEvent ??= nextOutput(connection.output);
			let events = [outputEvent];
			if (inputEvent !== null) events.push(inputEvent);
			if (drainEvent !== null) events.push(drainEvent);
			let event = await Promise.race(events);
			if (event.kind === "drained") {
				drainEvent = null;
				continue;
			}
			if (event.kind === "input_error") {
				throw event.error;
			}
			if (event.kind === "input") {
				inputEvent = null;
				if (event.result.done) {
					inputEnded = true;
					pending.push({
						data: { kind: "end", value: { combinedPosition, streamPositions } },
						end: combinedPosition,
						sent: false,
					});
					continue;
				}
				let chunk = event.result.value;
				if (!arg.streams.includes(chunk.stream)) {
					throw new ProtocolError("invalid process stdio stream");
				}
				if (chunk.bytes.length === 0) {
					complete?.(chunk);
					continue;
				}

				// Split the chunk, completing it once its last part is written.
				for (
					let offset = 0;
					offset < chunk.bytes.length;
					offset += tg.Process.Stdio.MAX_CHUNK_SIZE
				) {
					let length = Math.min(
						tg.Process.Stdio.MAX_CHUNK_SIZE,
						chunk.bytes.length - offset,
					);
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
					let last = offset + length === chunk.bytes.length;
					pending.push({
						data: { kind: "chunk", value },
						end: combinedPosition,
						...(last ? { original: chunk } : {}),
						sent: false,
					});
				}
				continue;
			}
			outputEvent = null;
			if (
				event.kind === "output_error" &&
				!isRetryableStreamError(event.error)
			) {
				throw event.error;
			}
			if (event.kind === "output_error" || event.result.done) {
				let error = event.kind === "output_error" ? event.error : undefined;
				drainEvent = null;
				[connection, reconnects] = await reconnect(
					client,
					id,
					arg,
					connection,
					reconnects,
					error,
				);
				for (let value of pending) {
					value.sent = false;
				}
				continue;
			}
			let { closed, position } = event.result.value;
			if (!Number.isSafeInteger(position) || position < 0) {
				throw new ProtocolError("invalid process stdio write position");
			}
			reconnects = 0;
			written = Math.max(written, position);
			while (
				pending[0] !== undefined &&
				pending[0].data.kind === "chunk" &&
				pending[0].end <= written
			) {
				let value = pending.shift()!;
				if (value.original !== undefined) {
					complete?.(value.original);
				}
			}
			if (closed) {
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
	output: AsyncIterableIterator<tg.Process.Stdio.Write.Output>,
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
	return await retry(
		client.streamRetry,
		() => writeProcessStdioOnce(client, id, arg),
		isRetryableError,
	);
}

/** Reconnect after the given number of consecutive failed attempts, returning the new connection and the number of consecutive failed attempts including this one. */
async function reconnect(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Write.Stream.Arg,
	connection: Connection,
	attempt: number,
	error: unknown,
): Promise<[Connection, number]> {
	connection.input.close();
	await connection.output.return?.();
	while (true) {
		// Fail once the consecutive attempts exhaust the stream retry options.
		if (attempt >= client.streamRetry.maxRetries) {
			throw (
				error ?? new Error("the process stdio stream ended before completion")
			);
		}

		// Back off, so that a stream that ends at once does not reconnect in a tight loop.
		await backoff(client.streamRetry, attempt);
		attempt++;

		// Connect.
		let next: Connection | null;
		try {
			next = await writeProcessStdioOnce(client, id, arg);
		} catch (error_) {
			if (!isRetryableError(error_)) {
				throw error_;
			}
			error = error_;
			continue;
		}
		if (next === null) {
			throw new Error("failed to find the process");
		}

		return [next, attempt];
	}
}

async function writeProcessStdioOnce(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Stdio.Write.Stream.Arg,
): Promise<Connection | null> {
	let input = new Channel<tg.Process.Stdio.Write.Data>();
	let uri = `/processes/${percentEncode(id)}/stdio/write`;
	let request = new Request({
		body: Body.sse(encodeData(input)),
		headers: {
			accept: "text/event-stream",
			"content-type": "text/event-stream",
		},
		method: "POST",
		uri,
	}).arg({
		...arg,
		location:
			arg.location === undefined || arg.location === null
				? null
				: tg.Location.Arg.toDataString(arg.location),
		streams: arg.streams.join(","),
		tokens: arg.tokens ?? {},
	});

	// Write the request body immediately, without waiting for the response headers.
	let response = client.send(request);
	response.catch(() => {});
	let output = receiveServerMessages(response, input);

	return { input, output };
}

async function* receiveServerMessages(
	response: Promise<Response>,
	input: Channel<tg.Process.Stdio.Write.Data>,
): AsyncIterableIterator<tg.Process.Stdio.Write.Output> {
	let value = await response;
	if (value.status === 404) {
		value.close();
		input.close();
		throw new NotFoundError("failed to find the process");
	}
	if (value.status < 200 || value.status >= 300) {
		input.close();
		throw await responseError(value);
	}
	let contentType = value.headers.get("content-type")?.split(";", 1)[0];
	if (contentType !== "text/event-stream") {
		input.close();
		throw new ProtocolError("invalid process stdio response content type");
	}
	yield* decodeOutputs(value);
}

async function* encodeData(
	input: AsyncIterable<tg.Process.Stdio.Write.Data>,
): AsyncIterableIterator<Body.SseEvent> {
	for await (let data of input) {
		yield {
			data: JSON.stringify(tg.Process.Stdio.Write.Data.toData(data)),
			event: "data",
		};
	}
}

async function* decodeOutputs(
	response: Response,
): AsyncIterableIterator<tg.Process.Stdio.Write.Output> {
	for await (let event of response.sse()) {
		try {
			if (event.event === "error") {
				throw errorFromData(
					JSON.parse(event.data) as tg.Error.Data | tg.Error.Id,
				);
			}
			if (event.event !== "output") {
				throw new ProtocolError("invalid process stdio write message");
			}
			let value = JSON.parse(event.data) as tg.Process.Stdio.Write.Output;
			yield { closed: value.closed, position: value.position };
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

function isRetryableError(error: unknown): boolean {
	return (
		!(error instanceof ProtocolError) &&
		!(error instanceof NotFoundError) &&
		isTransportError(error)
	);
}

// Retry an error from an open stream unless the server sent it, because any other failure of an open stream is a transport failure.
function isRetryableStreamError(error: unknown): boolean {
	return (
		!(error instanceof ProtocolError) &&
		!(error instanceof NotFoundError) &&
		(!(error instanceof tg.Error) || isTransportError(error))
	);
}

class Channel<T> implements AsyncIterableIterator<T> {
	#closed = false;
	#drained: Array<() => void> = [];
	#values: Array<T> = [];
	#waiters: Array<(result: IteratorResult<T>) => void> = [];

	get size(): number {
		return this.#values.length;
	}

	drained(): Promise<void> {
		if (this.#values.length === 0 || this.#closed) {
			return Promise.resolve();
		}
		return new Promise((resolve) => this.#drained.push(resolve));
	}

	close(): void {
		if (this.#closed) {
			return;
		}
		this.#closed = true;
		while (this.#drained.length > 0) {
			this.#drained.shift()!();
		}
		while (this.#waiters.length > 0) {
			this.#waiters.shift()!({ done: true, value: undefined });
		}
	}

	next(): Promise<IteratorResult<T>> {
		let value = this.#values.shift();
		if (value !== undefined) {
			if (this.#values.length === 0) {
				while (this.#drained.length > 0) {
					this.#drained.shift()!();
				}
			}
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
