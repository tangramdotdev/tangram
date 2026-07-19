import * as tg from "../../index.ts";
import { Request, Uri, percentEncode } from "../../http.ts";
import type { Client } from "../../client.ts";

export namespace Cancel {
	export type Arg = {
		lease: string;
		location?: tg.Location.Arg | null;
	};
}

export async function cancelProcess(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Cancel.Arg,
): Promise<void> {
	let found = await tryCancelProcess(client, id, arg);
	if (!found) {
		throw new Error("failed to find the process");
	}
}

export async function tryCancelProcess(
	client: Client,
	id: tg.Process.Id,
	arg: tg.Process.Cancel.Arg,
): Promise<true | null> {
	let method = "POST";
	let uri = new Uri({
		path: `/processes/${percentEncode(id)}/cancel`,
		query: {
			lease: arg.lease,
			location:
				arg.location === undefined || arg.location === null
					? null
					: tg.Location.Arg.toDataString(arg.location),
		},
	});
	let request = new Request({ method, uri });
	let response = await client.sendWithRetry(request);
	if (response.status === 404) {
		return null;
	} else if (response.status < 200 || response.status >= 300) {
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	return true;
}
