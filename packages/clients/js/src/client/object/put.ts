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
			children:
				arg.children?.map((child) =>
					tg.Referent.toDataString(child, (id) => id),
				) ?? null,
			location:
				arg.location === undefined || arg.location === null
					? null
					: tg.Location.Arg.toDataString(arg.location),
		},
	});
	let headers = {
		accept: "application/json",
		"content-type": "application/json",
	};
	let body = Body.json(arg.data);
	let request = new Request({
		body,
		method,
		uri,
		headers,
	});
	let response = await client.sendWithRetry(request);
	if (response.status < 200 || response.status >= 300) {
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	let output = await response.json<{ object: string }>();
	return {
		object: tg.Referent.fromDataString(
			output.object,
			(id) => id as tg.Object.Id,
		),
	};
}
