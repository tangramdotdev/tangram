import * as tg from "../../index.ts";
import { Request, Uri, percentEncode } from "../../http.ts";
import type { Client } from "../../client.ts";

export async function getSandbox(
	client: Client,
	id: tg.Sandbox.Id,
	arg?: tg.Sandbox.Get.Arg | null,
): Promise<tg.Sandbox.Get.Output> {
	let output = await tryGetSandbox(client, id, arg);
	if (output === null) {
		throw new Error("failed to find the sandbox");
	}
	return output;
}

export async function tryGetSandbox(
	client: Client,
	id: tg.Sandbox.Id,
	arg?: tg.Sandbox.Get.Arg | null,
): Promise<tg.Sandbox.Get.Output | null> {
	let method = "GET";
	let uri = new Uri({
		path: `/sandboxes/${percentEncode(id)}`,
		query: {
			location:
				arg?.location === undefined || arg.location === null
					? null
					: tg.Location.Arg.toDataString(arg.location),
		},
	});
	let headers = {
		accept: "application/json",
	};
	let request = new Request({
		method,
		uri,
		headers,
	});
	let response = await client.sendWithRetry(request);
	if (response.status === 404) {
		return null;
	} else if (response.status < 200 || response.status >= 300) {
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	let output = await response.json<
		Omit<tg.Sandbox.Get.Output, "location"> & {
			location?: string | tg.Location | null;
		}
	>();
	if (typeof output.location === "string") {
		output.location = tg.Location.fromDataString(output.location);
	}
	return output as tg.Sandbox.Get.Output;
}
