import * as tg from "../../index.ts";
import { Body, Request, Uri, percentEncode } from "../../http.ts";
import type { Client } from "../../client.ts";

export async function putObject(
	client: Client,
	id: tg.Object.Id,
	arg: tg.Object.Put.Arg,
): Promise<tg.Object.Put.Output> {
	let method = "PUT";
	let uri = new Uri({
		path: `/objects/${percentEncode(id)}`,
		query: {
			children: arg.children,
			location:
				arg.location === undefined
					? undefined
					: tg.Location.Arg.toDataString(arg.location),
		},
	});
	let headers = {
		accept: "application/json",
		"content-type": "application/json",
	};
	let body = Body.json(tg.Object.Data.toJson(arg.data));
	let request = new Request({
		body,
		method,
		uri,
		headers,
	});
	let response = await client.send(request);
	if (response.status < 200 || response.status >= 300) {
		let error: unknown;
		try {
			error = tg.Error.fromData(await response.json<tg.Error.Data>());
		} catch {
			error = new Error("the request failed");
		}
		throw error;
	}
	return await response.json<tg.Object.Put.Output>();
}
