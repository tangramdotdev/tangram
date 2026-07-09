import * as tg from "../../index.ts";
import { Body, Request, percentEncode } from "../../http.ts";
import type { Client } from "../../client.ts";

export namespace Put {
	export type Arg = {
		data: tg.Process.Data;
		location?: tg.Location.Arg | null;
	};

	export type Output = {
		token?: tg.Grant.Token | null;
	};
}

export async function putProcess(
	client: Client,
	id: tg.Process.Id,
	arg: Put.Arg,
): Promise<Put.Output> {
	let method = "PUT";
	let uri = `/processes/${percentEncode(id)}`;
	let headers = {
		accept: "application/json",
		"content-type": "application/json",
	};
	let body = Body.json({
		...arg,
		data: arg.data,
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
	if (response.status < 200 || response.status >= 300) {
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	return await response.json<Put.Output>();
}
