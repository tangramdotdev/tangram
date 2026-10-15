import * as tg from "../../../index.ts";
import { Body, Request } from "../../../http.ts";
import type { Client } from "../../../client.ts";

export async function setProcessTtySize(
	client: Client,
	arg: tg.Process.Tty.Put.Arg,
): Promise<void> {
	let found = await trySetProcessTtySize(client, arg);
	if (!found) {
		throw new Error("failed to find the process");
	}
}

export async function trySetProcessTtySize(
	client: Client,
	arg: tg.Process.Tty.Put.Arg,
): Promise<true | null> {
	let method = "PUT";
	let uri = "/processes/tty/size";
	let headers = {
		"content-type": "application/json",
	};
	let body = Body.json({
		...arg,
		location:
			arg.location === undefined || arg.location === null
				? null
				: tg.Location.Arg.toDataString(arg.location),
	});
	let request = new Request({
		body,
		method,
		uri,
		headers,
	});
	let response = await client.sendWithRetry(request);
	if (response.status === 404) {
		response.close();
		return null;
	} else if (response.status < 200 || response.status >= 300) {
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	response.close();
	return true;
}
