import * as tg from "../../index.ts";
import { Request, Uri, percentEncode } from "../../http.ts";
import type { Client } from "../../client.ts";

export namespace Get {
	export type Arg = {
		location?: tg.Location.Arg | undefined;
		metadata?: boolean | undefined;
	};

	export type Output = {
		data: tg.Process.Data;
		id: tg.Process.Id;
		location?: tg.Location | undefined;
		metadata?: unknown;
	};
}

export async function getProcess(
	client: Client,
	id: tg.Process.Id,
	arg?: Get.Arg | undefined,
): Promise<Get.Output> {
	let output = await tryGetProcess(client, id, arg);
	if (output === undefined) {
		throw new Error("failed to find the process");
	}
	return output;
}

export async function tryGetProcess(
	client: Client,
	id: tg.Process.Id,
	arg?: Get.Arg | undefined,
): Promise<Get.Output | undefined> {
	let method = "GET";
	let uri = new Uri({
		path: `/processes/${percentEncode(id)}`,
		query: {
			location:
				arg?.location === undefined
					? undefined
					: tg.Location.Arg.toDataString(arg.location),
			metadata: arg?.metadata === true ? true.toString() : undefined,
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
	let output = await response.json<
		Omit<Get.Output, "location"> & {
			location?: string | tg.Location | undefined;
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
