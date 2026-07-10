import * as tg from "../../index.ts";
import { Request, Uri, percentEncode } from "../../http.ts";
import type { Client } from "../../client.ts";

export namespace Wait {
	export type Arg = {
		lease?: string | null;
		location?: tg.Location.Arg | null;
		token?: tg.Grant.Token | null;
	};
}

export async function waitProcess(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Wait.Arg,
): Promise<tg.Process.Wait> {
	let promise = await waitProcessPromise(client, id, arg);
	let output = await promise();
	if (output === null) {
		throw new Error("failed to find the process");
	}
	return output;
}

export async function waitProcessPromise(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Wait.Arg,
): Promise<() => Promise<tg.Process.Wait | null>> {
	let promise = await tryWaitProcessPromise(client, id, arg);
	if (promise === null) {
		throw new Error("failed to find the process");
	}
	return promise;
}

export async function tryWaitProcessPromise(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Wait.Arg,
): Promise<(() => Promise<tg.Process.Wait | null>) | null> {
	return async () => {
		return await waitProcessLoop(client, id, arg);
	};
}

async function waitProcessLoop(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Wait.Arg,
): Promise<tg.Process.Wait | null> {
	while (true) {
		let output = await waitProcessOnce(client, id, arg);
		if (output !== null) {
			return output;
		}
	}
}

async function waitProcessOnce(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Wait.Arg,
): Promise<tg.Process.Wait | null> {
	let method = "POST";
	let uri = new Uri({
		path: `/processes/${percentEncode(id)}/wait`,
		query: {
			...arg,
			location:
				arg.location === undefined || arg.location === null
					? null
					: tg.Location.Arg.toDataString(arg.location),
		},
	});
	let headers = {
		accept: "text/event-stream",
	};
	let request = new Request({
		method,
		uri,
		headers,
	});
	let response = await client.send(request);
	if (response.status === 404) {
		throw new Error("failed to find the process");
	} else if (response.status < 200 || response.status >= 300) {
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	let output: tg.Process.Wait | null = null;
	for await (let event of response.sse()) {
		if (event.event === "output") {
			let data: tg.Process.Wait.Data = JSON.parse(event.data);
			output = tg.Process.Wait.fromData(data);
		} else if (event.event === "error") {
			let data = JSON.parse(event.data) as tg.Error.Data | tg.Error.Id;
			if (typeof data === "string") {
				throw tg.Error.withId(data);
			} else {
				throw tg.Error.fromData(data);
			}
		} else {
			throw new Error("invalid process wait event");
		}
	}
	return output;
}
