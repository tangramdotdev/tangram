import * as tg from "../index.ts";
import { Body, Request } from "../http.ts";
import type { Client } from "../client.ts";

export namespace Checkin {
	export type Arg = {
		options: Checkin.Options;
		path: string;
		updates: Array<string>;
	};

	export type Output = {
		artifact: tg.Referent<tg.Artifact.Id>;
	};

	export type Options = {
		cachePointers?: boolean | undefined;
		destructive?: boolean | undefined;
		deterministic?: boolean | undefined;
		root?: boolean | undefined;
		ignore?: boolean | undefined;
		localDependencies?: boolean | undefined;
		lock?: "auto" | "attr" | "file" | undefined;
		locked?: boolean | undefined;
		solve?: boolean | undefined;
		unsolvedDependencies?: boolean | undefined;
		ttl?: number | undefined;
		watch?: boolean | undefined;
	};
}

export async function checkin(
	client: Client,
	arg: tg.Checkin.Arg,
): Promise<AsyncIterableIterator<tg.Progress.Event<tg.Checkin.Output>>> {
	let method = "POST";
	let uri = "/checkin";
	let headers = {
		accept: "text/event-stream",
		"content-type": "application/json",
	};
	let body = Body.json({
		...arg,
		options: {
			cache_pointers: arg.options.cachePointers,
			destructive: arg.options.destructive,
			deterministic: arg.options.deterministic,
			ignore: arg.options.ignore,
			lock: arg.options.lock,
			locked: arg.options.locked,
			root: arg.options.root,
			solve: arg.options.solve,
			source_dependencies: arg.options.localDependencies,
			tag_ttl: arg.options.ttl,
			unsolved_dependencies: arg.options.unsolvedDependencies,
			watch: arg.options.watch,
		},
		updates: arg.updates.join(","),
	});
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
	return tg.Progress.decode<tg.Checkin.Output>(response);
}
