import * as tg from "../../index.ts";
import { Body, Request } from "../../http.ts";
import type { Client } from "../../client.ts";

export async function createSandbox(
	client: Client,
	arg: tg.Sandbox.Create.Arg,
): Promise<tg.Sandbox.Create.Output> {
	let method = "POST";
	let uri = "/sandboxes";
	let headers = {
		accept: "application/json",
		"content-type": "application/json",
	};
	let body = Body.json(arg);
	let request = new Request({
		body,
		headers,
		method,
		uri,
	});
	let response = await client.sendWithRetry(request);
	if (response.status < 200 || response.status >= 300) {
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	return await response.json<tg.Sandbox.Create.Output>();
}
