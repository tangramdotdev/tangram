import * as tg from "../index.ts";
import { Body, Request, Uri } from "../http.ts";
import type { Client } from "../client.ts";

export namespace Write {
	export type Arg = {
		cachePointers?: boolean;
	};

	export type Output = {
		blob: tg.Blob.Id;
	};
}

export async function write(
	client: Client,
	argOrBytes: tg.Write.Arg | string | Uint8Array,
	input?: AsyncIterableIterator<Uint8Array>,
): Promise<tg.Write.Output | tg.Blob.Id> {
	if (typeof argOrBytes === "string" || argOrBytes instanceof Uint8Array) {
		let output = await write(
			client,
			{},
			singleBytes(
				typeof argOrBytes === "string"
					? tg.encoding.utf8.encode(argOrBytes)
					: argOrBytes,
			),
		);
		return (output as tg.Write.Output).blob;
	}
	let method = "POST";
	let uri = new Uri({
		path: "/write",
		query: {
			cache_pointers:
				argOrBytes.cachePointers === undefined
					? undefined
					: argOrBytes.cachePointers.toString(),
		},
	});
	let headers = {
		accept: "application/json",
		"content-type": "application/octet-stream",
	};
	tg.assert(input !== undefined);
	let body = new Body(input);
	let request = new Request({
		body,
		method,
		uri,
		headers,
	});
	let response = await client.send(request);
	if (response.status < 200 || response.status >= 300) {
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	return await response.json<tg.Write.Output>();
}

async function* singleBytes(
	bytes: Uint8Array,
): AsyncIterableIterator<Uint8Array> {
	yield bytes;
}
