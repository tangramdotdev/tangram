import * as tg from "../../../index.ts";
import { Body, Request, percentEncode } from "../../../http.ts";
import type { Client } from "../../../client.ts";

export async function setProcessTtySize(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Tty.Put.Arg,
): Promise<void> {
	let found = await trySetProcessTtySize(client, id, arg);
	if (!found) {
		throw new Error("failed to find the process");
	}
}

export async function trySetProcessTtySize(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Tty.Put.Arg,
): Promise<true | null> {
	let method = "PUT";
	let uri = `/processes/${percentEncode(id)}/tty/size`;
	let headers = {
		"content-type": "application/json",
	};
	let body = Body.json({
		...arg,
		location:
			arg.location == null
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
	if (response.status === 404) {
		return null;
	} else if (response.status < 200 || response.status >= 300) {
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	return true;
}
