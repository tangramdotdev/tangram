import * as tg from "../../index.ts";
import { Body, Request, percentEncode } from "../../http.ts";
import type { Client } from "../../client.ts";

export async function stopSandbox(
	client: Client,
	id: tg.Sandbox.Id,
	arg?: tg.Sandbox.Stop.Arg | null,
): Promise<void> {
	let stopped = await tryStopSandbox(client, id, arg);
	if (stopped === null) {
		throw new Error("failed to find the sandbox");
	}
}

export async function tryStopSandbox(
	client: Client,
	id: tg.Sandbox.Id,
	arg?: tg.Sandbox.Stop.Arg | null,
): Promise<boolean | null> {
	let method = "POST";
	let uri = `/sandboxes/${percentEncode(id)}/stop`;
	let headers = {
		"content-type": "application/json",
	};
	let body = Body.json({
		location:
			arg?.location === undefined || arg.location === null
				? null
				: tg.Location.Arg.toDataString(arg.location),
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
