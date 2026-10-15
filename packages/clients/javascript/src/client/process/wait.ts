import * as tg from "../../index.ts";
import { Request } from "../../http.ts";
import type { Client } from "../../client.ts";

export namespace Wait {
	export type Arg = {
		lease?: string | null;
		location?: tg.Location.Arg | null;
		process: tg.Process.Id;
		source?: tg.Process.Source;
		tokens?: tg.Authorization.Tokens | null;
	};
}

export async function waitProcess(
	client: Client,
	arg: tg.Process.Wait.Arg,
): Promise<tg.Process.Outcome> {
	let promise = await waitProcessPromise(client, arg);
	let outcome = await promise();
	if (outcome === null) {
		throw new Error("failed to find the process");
	}
	return outcome;
}

export async function waitProcessPromise(
	client: Client,
	arg: tg.Process.Wait.Arg,
): Promise<() => Promise<tg.Process.Outcome | null>> {
	let promise = await tryWaitProcessPromise(client, arg);
	if (promise === null) {
		throw new Error("failed to find the process");
	}
	return promise;
}

export async function tryWaitProcessPromise(
	client: Client,
	arg: tg.Process.Wait.Arg,
): Promise<(() => Promise<tg.Process.Outcome | null>) | null> {
	return async () => {
		return await waitProcessLoop(client, arg);
	};
}

async function waitProcessLoop(
	client: Client,
	arg: tg.Process.Wait.Arg,
): Promise<tg.Process.Outcome | null> {
	while (true) {
		let outcome = await waitProcessOnce(client, arg);
		if (outcome !== null) {
			return outcome;
		}
	}
}

async function waitProcessOnce(
	client: Client,
	arg: tg.Process.Wait.Arg,
): Promise<tg.Process.Outcome | null> {
	let method = "POST";
	let uri = "/processes/wait";
	let headers = {
		accept: "text/event-stream",
	};
	let request = new Request({
		method,
		uri,
		headers,
	}).arg({
		...arg,
		location:
			arg.location === undefined || arg.location === null
				? null
				: tg.Location.Arg.toDataString(arg.location),
		tokens: arg.tokens ?? {},
	});
	let response = await client.sendWithRetry(request);
	if (response.status === 404) {
		response.close();
		throw new Error("failed to find the process");
	} else if (response.status < 200 || response.status >= 300) {
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	let outcome: tg.Process.Outcome | null = null;
	for await (let event of response.sse()) {
		if (event.event === "outcome") {
			let data: tg.Process.Outcome.Data = JSON.parse(event.data);
			outcome = tg.Process.Outcome.fromData(data);
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
	return outcome;
}
