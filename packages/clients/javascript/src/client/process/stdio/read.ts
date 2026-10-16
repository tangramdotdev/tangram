import * as tg from "../../../index.ts";
import { Body, Request, Response, percentEncode } from "../../../http.ts";
import {
	type Client,
	backoff,
	isTransportError,
	retry,
} from "../../../client.ts";

export type Connection = {
	close: () => void;
	output: AsyncIterableIterator<tg.Process.Stdio.Read.ServerMessage>;
};

class ProtocolError extends Error {}

/** The position through which the reader has the read's bytes, which each connection sends so the runner can release them. */
class Acks {
	#closed = false;
	#position: number;
	#waiters: Array<() => void> = [];

	constructor(position: number) {
		this.#position = position;
	}

	close() {
		this.#closed = true;
		this.#wake();
	}

	set(position: number) {
		if (position <= this.#position) {
			return;
		}
		this.#position = position;
		this.#wake();
	}

	async *events(): AsyncIterableIterator<Body.SseEvent> {
		let sent: number | null = null;
		while (true) {
			if (sent !== this.#position) {
				sent = this.#position;
				yield { data: JSON.stringify(sent), event: "ack" };
				continue;
			}
			if (this.#closed) {
				return;
			}
			await new Promise<void>((resolve) => this.#waiters.push(resolve));
		}
	}

	#wake() {
		for (let waiter of this.#waiters.splice(0)) {
			waiter();
		}
	}
}

export async function tryReadProcessStdio(
	client: Client,
	id: tg.Process.Nonce | tg.Process.Id,
	arg: tg.Process.Stdio.Read.Arg,
): Promise<AsyncIterableIterator<tg.Process.Stdio.Chunk> | null> {
	let acks = new Acks(typeof arg.position === "number" ? arg.position : 0);
	let connection = await connect(client, id, arg, acks);
	if (connection === null) {
		acks.close();
		return null;
	}

	return readProcessStdioAll(client, id, arg, connection, acks);
}

export function readProcessStdioAll(
	client: Client,
	id: tg.Process.Nonce | tg.Process.Id,
	arg: tg.Process.Stdio.Read.Arg,
	connection: Connection,
	acks: Acks,
): AsyncIterableIterator<tg.Process.Stdio.Chunk> {
	let state = { canceled: false, connection };
	let output = readProcessStdioAllInner(client, id, arg, state, acks);
	return {
		next: () => output.next(),
		return: async () => {
			// Close the response, so that a pending next settles and the read ends now.
			state.canceled = true;
			acks.close();
			state.connection.close();
			return await output.return!();
		},
		[Symbol.asyncIterator]() {
			return this;
		},
	};
}

async function* readProcessStdioAllInner(
	client: Client,
	id: tg.Process.Nonce | tg.Process.Id,
	arg: tg.Process.Stdio.Read.Arg,
	state: { canceled: boolean; connection: Connection },
	acks: Acks,
): AsyncIterableIterator<tg.Process.Stdio.Chunk> {
	let connection = state.connection;
	let combined = arg.streams.length > 1;
	let forward =
		arg.length === undefined || arg.length === null || arg.length >= 0;
	let nextArg = { ...arg, streams: [...arg.streams] };
	let position = typeof arg.position === "string" ? null : (arg.position ?? 0);
	let reconnects = 0;
	try {
		while (!state.canceled) {
			let result: IteratorResult<tg.Process.Stdio.Read.ServerMessage> | null =
				null;
			let error: unknown;
			try {
				result = await connection.output.next();
			} catch (error_) {
				if (state.canceled) return;
				if (!isRetryableStreamError(error_)) {
					throw error_;
				}
				error = error_;
			}
			if (state.canceled) return;
			if (result === null || result.done) {
				[connection, reconnects] = await reconnect(
					client,
					id,
					nextArg,
					connection,
					acks,
					reconnects,
					error,
				);
				state.connection = connection;
				continue;
			}
			let message = result.value;
			if (message.kind === "response") {
				tg.Process.Stdio.Read.Output.validate(
					message.value,
					arg.streams,
					position ?? 0,
				);
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
			reconnects = 0;
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

			// The consumer has the chunk once it is yielded, so the runner no longer needs to keep it.
			if (forward) {
				acks.set(position);
			}
			yield chunk;
		}
	} finally {
		acks.close();
		await connection.output.return?.();
	}
}

async function connect(
	client: Client,
	id: tg.Process.Nonce | tg.Process.Id,
	arg: tg.Process.Stdio.Read.Arg,
	acks: Acks,
): Promise<Connection | null> {
	return await retry(
		client.streamRetry,
		() => readProcessStdioOnce(client, id, arg, acks),
		isRetryableError,
	);
}

/** Reconnect after the given number of consecutive failed attempts, returning the new connection and the number of consecutive failed attempts including this one. */
async function reconnect(
	client: Client,
	id: tg.Process.Nonce | tg.Process.Id,
	arg: tg.Process.Stdio.Read.Arg,
	connection: Connection,
	acks: Acks,
	attempt: number,
	error: unknown,
): Promise<[Connection, number]> {
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
			next = await readProcessStdioOnce(client, id, arg, acks);
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

async function readProcessStdioOnce(
	client: Client,
	id: tg.Process.Nonce | tg.Process.Id,
	arg: tg.Process.Stdio.Read.Arg,
	acks: Acks,
): Promise<Connection | null> {
	let uri = `/processes/${percentEncode(id)}/stdio/read`;
	let request = new Request({
		body: Body.sse(acks.events()),
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
	let response = await client.send(request);
	if (response.status === 404) {
		response.close();
		return null;
	}
	if (response.status < 200 || response.status >= 300) {
		throw await responseError(response);
	}
	let contentType = response.headers.get("content-type")?.split(";", 1)[0];
	if (contentType !== "text/event-stream") {
		throw new ProtocolError("invalid process stdio response content type");
	}
	let output = decodeServerMessages(response);
	let close = () => response.close();

	return { close, output };
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

function isRetryableError(error: unknown): boolean {
	return !(error instanceof ProtocolError) && isTransportError(error);
}

// Retry an error from an open stream unless the server sent it, because any other failure of an open stream is a transport failure.
function isRetryableStreamError(error: unknown): boolean {
	return (
		!(error instanceof ProtocolError) &&
		(!(error instanceof tg.Error) || isTransportError(error))
	);
}
