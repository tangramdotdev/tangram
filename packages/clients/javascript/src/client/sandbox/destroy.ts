import * as tg from "../../index.ts";
import { Body, Request } from "../../http.ts";
import type { Client } from "../../client.ts";

export async function destroySandbox(
	client: Client,
	arg: tg.Sandbox.Destroy.Arg,
): Promise<void> {
	let destroyed = await tryDestroySandbox(client, arg);
	if (destroyed === null) {
		throw new Error("failed to find the sandbox");
	} else if (!destroyed) {
		throw new Error("the sandbox was already destroyed");
	}
}

export async function tryDestroySandbox(
	client: Client,
	arg: tg.Sandbox.Destroy.Arg,
): Promise<boolean | null> {
	let method = "POST";
	let uri = "/sandboxes/destroy";
	let headers = {
		"content-type": "application/json",
	};
	let body = Body.json({
		location:
			arg.location === undefined || arg.location === null
				? null
				: tg.Location.Arg.toDataString(arg.location),
		sandbox: arg.sandbox,
	});
	let request = new Request({
		body,
		headers,
		method,
		uri,
	});
	let response = await client.sendWithRetry(request);
	if (response.status === 404) {
		response.close();
		return null;
	} else if (response.status === 409) {
		response.close();
		return false;
	} else if (response.status < 200 || response.status >= 300) {
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	response.close();
	return true;
}
