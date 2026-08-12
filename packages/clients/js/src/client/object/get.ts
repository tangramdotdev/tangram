import * as tg from "../../index.ts";
import { Request, Uri, percentEncode } from "../../http.ts";
import type { Client } from "../../client.ts";

export async function getObject(
	client: Client,
	id: tg.Object.Id,
	arg?: tg.Object.Get.Arg | null,
): Promise<tg.Object.Get.Output> {
	let output = await tryGetObject(client, id, arg);
	if (output === null) {
		throw await tg.error`failed to find the object`.values({ id });
	}
	return output;
}

export async function tryGetObject(
	client: Client,
	id: tg.Object.Id,
	arg?: tg.Object.Get.Arg | null,
): Promise<tg.Object.Get.Output | null> {
	let method = "GET";
	let uri = new Uri({
		path: `/objects/${percentEncode(id)}`,
		query: {
			location:
				arg?.location === undefined || arg.location === null
					? null
					: tg.Location.Arg.toDataString(arg.location),
			metadata: arg?.metadata === true ? true.toString() : null,
			tokens: arg?.tokens ?? null,
		},
	});
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
	let data = await response.json<tg.Object.Data>();
	let tokens = response.headers.get("x-tg-object-tokens");
	let output = {
		data,
		...(tokens === undefined
			? {}
			: { tokens: JSON.parse(tokens) as tg.Grant.Tokens }),
	};
	return output;
}
