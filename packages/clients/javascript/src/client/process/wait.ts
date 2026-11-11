import * as tg from "../../index.ts";
import { Request, percentEncode } from "../../http.ts";
import { backoff, isTransportError, type Client } from "../../client.ts";

export namespace Wait {
	export type Arg = {
		lease?: string | null;
		location?: tg.Location.Arg | null;
		tokens?: tg.Authorization.Tokens.Data | null;
	};
}

export async function waitProcess(
	client: Client,
	id: tg.Process.Nonce | tg.Process.Id,
	arg: tg.Process.Wait.Arg,
): Promise<tg.Process.Outcome> {
	let promise = await waitProcessPromise(client, id, arg);
	let outcome = await promise();
	if (outcome === null) {
		throw new Error("failed to find the process");
	}
	return outcome;
}

export async function waitProcessPromise(
	client: Client,
	id: tg.Process.Nonce | tg.Process.Id,
	arg: tg.Process.Wait.Arg,
): Promise<() => Promise<tg.Process.Outcome | null>> {
	let promise = await tryWaitProcessPromise(client, id, arg);
	if (promise === null) {
		throw new Error("failed to find the process");
	}
	return promise;
}

export async function tryWaitProcessPromise(
	client: Client,
	id: tg.Process.Nonce | tg.Process.Id,
	arg: tg.Process.Wait.Arg,
): Promise<(() => Promise<tg.Process.Outcome | null>) | null> {
	return async () => {
		return await waitProcessLoop(client, id, arg);
	};
}

async function waitProcessLoop(
	client: Client,
	id: tg.Process.Nonce | tg.Process.Id,
	arg: tg.Process.Wait.Arg,
): Promise<tg.Process.Outcome | null> {
	// Wait again after an end without an outcome, or after an early end with backoff.
	let attempt = 0;
	while (true) {
		let { ended, outcome } = await waitProcessOnce(client, id, arg);
		if (ended) {
			if (outcome !== null) {
				return outcome;
			}
			continue;
		}
		if (attempt >= client.streamRetry.maxRetries) {
			throw new Error("the process wait stream ended before its end event");
		}
		await backoff(client.streamRetry, attempt);
		attempt += 1;
	}
}

async function waitProcessOnce(
	client: Client,
	id: tg.Process.Nonce | tg.Process.Id,
	arg: tg.Process.Wait.Arg,
): Promise<{ ended: boolean; outcome: tg.Process.Outcome | null }> {
	let method = "POST";
	let uri = `/processes/${percentEncode(id)}/wait`;
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
	// Read the events, which a lost connection ends early and an error event fails.
	let outcome: tg.Process.Outcome | null = null;
	let events = response.sse()[Symbol.asyncIterator]();
	while (true) {
		let next;
		try {
			next = await events.next();
		} catch (error) {
			if (isTransportError(error)) {
				return { ended: false, outcome: null };
			}
			throw error;
		}
		if (next.done) {
			return { ended: false, outcome: null };
		}
		let event = next.value;
		if (event.event === "end") {
			return { ended: true, outcome };
		} else if (event.event === "outcome") {
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
}
