import * as tg from "../index.ts";
import { Request, Response, Uri } from "../http.ts";
import type { Client } from "../client.ts";

export namespace Read {
	export type Arg = {
		blob: tg.Blob.Id;
		token?: tg.Grant.Token;
	} & Read.Options;

	export type Options = {
		position?: number | string;
		length?: number;
		size?: number;
	};

	export type Event =
		| { kind: "chunk"; value: Read.Event.Chunk }
		| { kind: "end" };

	export namespace Event {
		export type Chunk = {
			bytes: Uint8Array;
			position: number;
		};
	}
}

export async function read(
	client: Client,
	arg: tg.Read.Arg,
): Promise<Uint8Array> {
	let stream = await tryReadStream(client, arg);
	if (stream === undefined) {
		throw new Error("failed to find the blob");
	}
	return await collectReadStream(stream);
}

export async function tryRead(
	client: Client,
	arg: tg.Read.Arg,
): Promise<Uint8Array | undefined> {
	let stream = await tryReadStream(client, arg);
	return stream === undefined ? undefined : await collectReadStream(stream);
}

export async function tryReadStream(
	client: Client,
	arg: tg.Read.Arg,
): Promise<AsyncIterableIterator<tg.Read.Event> | undefined> {
	let method = "GET";
	let uri = new Uri({
		path: "/read",
		query: {
			blob: arg.blob,
			length: arg.length,
			position: arg.position,
			size: arg.size,
			token: arg.token,
		},
	});
	let headers = {
		accept: "application/octet-stream",
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
	return decodeReadEvents(response);
}

async function collectReadStream(
	stream: AsyncIterable<tg.Read.Event>,
): Promise<Uint8Array> {
	let chunks: Array<Uint8Array> = [];
	let length = 0;
	for await (let event of stream) {
		if (event.kind === "end") {
			break;
		}
		chunks.push(event.value.bytes);
		length += event.value.bytes.length;
	}
	let output = new Uint8Array(length);
	let position = 0;
	for (let chunk of chunks) {
		output.set(chunk, position);
		position += chunk.length;
	}
	return output;
}

async function* decodeReadEvents(
	response: Response,
): AsyncIterableIterator<tg.Read.Event> {
	let position = response.headers.get("x-tg-position");
	let nextPosition = position === undefined ? undefined : Number(position);
	if (nextPosition !== undefined && !Number.isInteger(nextPosition)) {
		throw new Error("expected an integer");
	}
	for await (let bytes of response.body) {
		if (nextPosition === undefined) {
			throw new Error("expected a position");
		}
		yield {
			kind: "chunk",
			value: {
				bytes,
				position: nextPosition,
			},
		};
		nextPosition += bytes.length;
	}
	yield { kind: "end" };
}
