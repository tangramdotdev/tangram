import * as tg from "../../index.ts";
import { Request, percentEncode } from "../../http.ts";
import type { Client } from "../../client.ts";

export namespace Cancel {
	export type Arg = {
		lease: string;
		location?: tg.Location.Arg | null;
	};

	export type Output = {
		released: boolean;
	};
}

export async function cancelProcess(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Cancel.Arg,
): Promise<tg.Process.Cancel.Output> {
	let output = await tryCancelProcess(client, id, arg);
	if (output === null) {
		throw new Error("failed to find the process");
	}
	return output;
}

export async function tryCancelProcess(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Cancel.Arg,
): Promise<tg.Process.Cancel.Output | null> {
	let method = "POST";
	let uri = `/processes/${percentEncode(id)}/cancel`;
	let request = new Request({ method, uri }).arg({
		lease: arg.lease,
		location:
			arg.location === undefined || arg.location === null
				? null
				: tg.Location.Arg.toDataString(arg.location),
	});
	let response = await client.sendWithRetry(request);
	if (response.status === 404) {
		return null;
	} else if (response.status < 200 || response.status >= 300) {
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	return await response.json<tg.Process.Cancel.Output>();
}
