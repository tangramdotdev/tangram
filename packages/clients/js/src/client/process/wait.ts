import * as tg from "../../index.ts";
import { Request, Uri, percentEncode } from "../../http.ts";
import type { Client } from "../../client.ts";

export namespace Wait {
	export type Arg = {
		lease: string | undefined;
		location?: tg.Location.Arg | undefined;
		token?: tg.Grant.Token | undefined;
	};
}

export async function waitProcess(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Wait.Arg,
): Promise<tg.Process.Wait> {
	let promise = await waitProcessPromise(client, id, arg);
	let output = await promise();
	if (output === undefined) {
		throw new Error("failed to find the process");
	}
	return output;
}

export async function waitProcessPromise(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Wait.Arg,
): Promise<() => Promise<tg.Process.Wait | undefined>> {
	let promise = await tryWaitProcessPromise(client, id, arg);
	if (promise === undefined) {
		throw new Error("failed to find the process");
	}
	return promise;
}

export async function tryWaitProcessPromise(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Wait.Arg,
): Promise<(() => Promise<tg.Process.Wait | undefined>) | undefined> {
	return async () => {
		return await waitProcessLoop(client, id, arg);
	};
}

async function waitProcessLoop(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Wait.Arg,
): Promise<tg.Process.Wait | undefined> {
	while (true) {
		let output = await waitProcessOnce(client, id, arg);
		if (output !== undefined) {
			return output;
		}
	}
}

async function waitProcessOnce(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Wait.Arg,
): Promise<tg.Process.Wait | undefined> {
	let method = "POST";
	let uri = new Uri({
		path: `/processes/${percentEncode(id)}/wait`,
		query: {
			...arg,
			location:
				arg.location === undefined
					? undefined
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
		let error: unknown;
		try {
			error = tg.Error.fromData(await response.json<tg.Error.Data>());
		} catch {
			error = new Error("the request failed");
		}
		throw error;
	}
	let output: tg.Process.Wait | undefined;
	for await (let event of response.sse()) {
		if (event.event === "output") {
			let data = JSON.parse(event.data) as tg.Process.Wait.Data;
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
