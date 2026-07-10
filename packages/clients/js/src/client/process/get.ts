import * as tg from "../../index.ts";
import { Request, Uri, percentEncode } from "../../http.ts";
import type { Client } from "../../client.ts";

export namespace Get {
	export type Arg = {
		location?: tg.Location.Arg | null;
		metadata?: boolean;
	};

	export type Output = {
		data: tg.Process.Data;
		id: tg.Process.Id;
		location?: tg.Location | null;
		metadata?: unknown;
	};
}

export async function getProcess(
	client: Client,
	id: tg.Process.Id,
	arg?: Get.Arg | null,
): Promise<Get.Output> {
	let output = await tryGetProcess(client, id, arg);
	if (output === null) {
		throw new Error("failed to find the process");
	}
	return output;
}

export async function tryGetProcess(
	client: Client,
	id: tg.Process.Id,
	arg?: Get.Arg | null,
): Promise<Get.Output | null> {
	let method = "GET";
	let uri = new Uri({
		path: `/processes/${percentEncode(id)}`,
		query: {
			location:
				arg?.location === undefined || arg.location === null
					? null
					: tg.Location.Arg.toDataString(arg.location),
			metadata: arg?.metadata === true ? true.toString() : null,
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
	let response = await client.send(request);
	if (response.status === 404) {
		return null;
	} else if (response.status < 200 || response.status >= 300) {
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	let output = await response.json<
		Omit<Get.Output, "location"> & {
			location?: string | tg.Location | null;
		}
	>();
	if (typeof output.location === "string") {
		output.location = tg.Location.fromDataString(output.location);
	}
	let metadata = response.headers.get("x-tg-process-metadata");
	if (metadata !== undefined) {
		output.metadata = JSON.parse(metadata) as unknown;
	}
	return output as Get.Output;
}
