import * as tg from "../index.ts";
import { Body, Request } from "../http.ts";
import type { Client } from "../client.ts";

export namespace Checkout {
	export type Arg = {
		artifact: tg.Artifact.Id;
		dependencies: boolean;
		extension?: string | undefined;
		force: boolean;
		lock?: "auto" | "attr" | "file" | undefined;
		path?: string | undefined;
	};

	export type Output = {
		path: string;
	};
}

export async function checkout(
	client: Client,
	arg: tg.Checkout.Arg,
): Promise<AsyncIterableIterator<tg.Progress.Event<tg.Checkout.Output>>> {
	let method = "POST";
	let uri = "/checkout";
	let headers = {
		accept: "text/event-stream",
		"content-type": "application/json",
	};
	let body = Body.json(arg);
	let request = new Request({
		body,
		method,
		uri,
		headers,
	});
	let response = await client.send(request);
	if (response.status < 200 || response.status >= 300) {
		let error: unknown;
		try {
			error = tg.Error.fromData(await response.json<tg.Error.Data>());
		} catch {
			error = new Error("the request failed");
		}
		throw error;
	}
	return tg.Progress.decode<tg.Checkout.Output>(response);
}
