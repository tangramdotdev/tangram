import * as tg from "../../index.ts";
import { Body, Request, percentEncode } from "../../http.ts";
import type { Client } from "../../client.ts";

export namespace Signal {
	export type Arg = {
		location?: tg.Location.Arg | undefined;
		signal: tg.Process.Signal;
	};
}

export async function signalProcess(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Signal.Arg,
): Promise<void> {
	let found = await trySignalProcess(client, id, arg);
	if (!found) {
		throw new Error("failed to find the process");
	}
}

export async function trySignalProcess(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Signal.Arg,
): Promise<true | undefined> {
	let method = "POST";
	let uri = `/processes/${percentEncode(id)}/signal`;
	let headers = {
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
	if (response.status === 404) {
		return undefined;
	} else if (response.status < 200 || response.status >= 300) {
		let error: unknown;
		try {
			error = tg.Error.fromData(await response.json<tg.Error.Data>());
		} catch {
			error = new Error("the request failed");
		}
		throw error;
	}
	return true;
}
