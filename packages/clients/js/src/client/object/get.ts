import * as tg from "../../index.ts";
import { Request, percentEncode } from "../../http.ts";
import type { Client } from "../../client.ts";

export async function getObject(
	client: Client,
	id: tg.Object.Id,
): Promise<tg.Object.Data> {
	let output = await tryGetObject(client, id);
	if (output === null) {
		throw new Error("failed to find the object");
	}
	return output;
}

export async function tryGetObject(
	client: Client,
	id: tg.Object.Id,
): Promise<tg.Object.Data | null> {
	let method = "GET";
	let uri = `/objects/${percentEncode(id)}`;
	let headers = {
		accept: "application/json",
	};
	let request = new Request({
		method,
		uri,
		headers,
	});
	let response = await client.sendWithRetry(request);
	if (response.status === 404) {
		return null;
	} else if (response.status < 200 || response.status >= 300) {
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	return await response.json<tg.Object.Data>();
}
