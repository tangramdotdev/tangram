import * as tg from "../../index.ts";
import { Body, Request } from "../../http.ts";
import type { Client } from "../../client.ts";

export async function postObjectBatch(
	client: Client,
	arg: tg.Object.Batch.Arg,
): Promise<tg.Object.Batch.Output> {
	let method = "POST";
	let uri = "/objects/batch";
	let headers = {
		accept: "application/json",
		"content-type": "application/json",
	};
	let body = Body.json({
		...arg,
		location:
			arg.location === undefined
				? undefined
				: tg.Location.Arg.toDataString(arg.location),
	});
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
	return await response.json<tg.Object.Batch.Output>();
}
