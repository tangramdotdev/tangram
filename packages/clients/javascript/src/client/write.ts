import * as tg from "../index.ts";
import { Body, Request } from "../http.ts";
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
		let body =
			typeof argOrBytes === "string"
				? Body.text(argOrBytes)
				: Body.bytes(argOrBytes);
		let output = await writeWithBody(client, {}, body);
		return output.blob.node;
	}
	tg.assert(input !== undefined);
	let body = new Body(input);
	return await writeWithBody(client, argOrBytes, body);
}

async function writeWithBody(
	client: Client,
	arg: tg.Write.Arg,
	body: Body,
): Promise<tg.Write.Output> {
	let method = "POST";
	let uri = "/write";
	let headers = {
		accept: "application/json",
		"content-type": "application/octet-stream",
	};
	let request = new Request({
		body,
		method,
		uri,
		headers,
	}).arg({
		checkout_pointers: arg.checkoutPointers ?? null,
	});
	let response = await (body.replayable
		? client.sendWithRetry(request)
		: client.send(request));
	if (response.status < 200 || response.status >= 300) {
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	let output = await response.json<{ blob: string }>();
	return {
		blob: tg.Referent.fromDataString(output.blob, (id) => id as tg.Blob.Id),
	};
}
