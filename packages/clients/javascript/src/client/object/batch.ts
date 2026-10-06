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
			arg.location === undefined || arg.location === null
				? null
				: tg.Location.Arg.toDataString(arg.location),
		objects: arg.objects.map((object) => ({
			...object,
			children: object.children?.map((child) =>
				tg.Referent.toDataString(child, (id) => id),
			),
			data: object.data,
		})),
	});
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
	let output = await response.json<{ objects: Array<string> }>();
	return {
		objects: output.objects.map((object) =>
			tg.Referent.fromDataString(object, (id) => id as tg.Object.Id),
		),
	};
}
