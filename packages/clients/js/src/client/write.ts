import * as tg from "../index.ts";
import { Body, Request, Uri } from "../http.ts";
import type { Client } from "../client.ts";

export namespace Write {
	export type Arg = {
		checkoutPointers?: boolean;
	};

	export type Output = {
		blob: tg.Referent<tg.Blob.Id>;
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
		return (output as tg.Write.Output).blob.node;
	}
	let method = "POST";
	let uri = new Uri({
		path: "/write",
		query: {
			checkout_pointers:
				argOrBytes.checkoutPointers === undefined
					? null
					: argOrBytes.checkoutPointers.toString(),
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
	let output = await response.json<{ blob: string }>();
	return {
		blob: tg.Referent.fromDataString(output.blob, (id) => id as tg.Blob.Id),
	};
}

async function* singleBytes(
	bytes: Uint8Array,
): AsyncIterableIterator<Uint8Array> {
	yield bytes;
}
