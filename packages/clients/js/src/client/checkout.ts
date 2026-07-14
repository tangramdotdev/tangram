import * as tg from "../index.ts";
import { Body, Request } from "../http.ts";
import type { Client } from "../client.ts";

export namespace Checkout {
	export type Arg = {
		artifact: tg.Artifact.Id;
		dependencies?: boolean;
		extension?: string | null;
		force?: boolean;
		lock?: "auto" | "attr" | "file" | null;
		path?: string | null;
	};

	export namespace Arg {
		export let toJson = (arg: tg.Checkout.Arg): unknown => {
			let output: { [key: string]: unknown } = {
				artifact: arg.artifact,
			};
			if (arg.dependencies !== undefined && !arg.dependencies) {
				output.dependencies = arg.dependencies;
			}
			if (arg.extension !== undefined) {
				output.extension = arg.extension;
			}
			if (arg.force) {
				output.force = arg.force;
			}
			if (arg.lock !== undefined) {
				if (arg.lock === null) {
					output.lock = null;
				} else if (arg.lock !== "auto") {
					output.lock = arg.lock;
				}
			}
			if (arg.path !== undefined) {
				output.path = arg.path;
			}
			return output;
		};
	}

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
	let body = Body.json(Checkout.Arg.toJson(arg));
	let request = new Request({
		body,
		method,
		uri,
		headers,
	});
	let response = await client.sendWithRetry(request);
	if (response.status < 200 || response.status >= 300) {
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	return tg.Progress.decode<tg.Checkout.Output>(response);
}
