import * as tg from "../index.ts";
import { Body, Request } from "../http.ts";
import type { Client } from "../client.ts";

export namespace Checkin {
	export type Arg = {
		options: Checkin.Options;
		path: string;
		updates: Array<string>;
	};

	export namespace Arg {
		export let toJson = (arg: tg.Checkin.Arg): unknown => {
			let options: { [key: string]: unknown } = {};
			if (!arg.options.cachePointers) {
				options.cache_pointers = arg.options.cachePointers;
			}
			if (arg.options.destructive) {
				options.destructive = arg.options.destructive;
			}
			if (arg.options.deterministic) {
				options.deterministic = arg.options.deterministic;
			}
			if (!arg.options.ignore) {
				options.ignore = arg.options.ignore;
			}
			if ("lock" in arg.options) {
				if (arg.options.lock === undefined) {
					options.lock = null;
				} else if (arg.options.lock !== "auto") {
					options.lock = arg.options.lock;
				}
			}
			if (arg.options.locked) {
				options.locked = arg.options.locked;
			}
			if (arg.options.root) {
				options.root = arg.options.root;
			}
			if (!arg.options.solve) {
				options.solve = arg.options.solve;
			}
			if (!arg.options.localDependencies) {
				options.source_dependencies = arg.options.localDependencies;
			}
			if (arg.options.ttl !== undefined) {
				options.tag_ttl = arg.options.ttl;
			}
			if (arg.options.unsolvedDependencies) {
				options.unsolved_dependencies = arg.options.unsolvedDependencies;
			}
			if (arg.options.watch) {
				options.watch = arg.options.watch;
			}
			let output: { [key: string]: unknown } = {
				options,
				path: arg.path,
			};
			if (arg.updates.length > 0) {
				output.updates = arg.updates.join(",");
			}
			return output;
		};
	}

	export type Output = {
		artifact: tg.Referent<tg.Artifact.Id>;
	};

	export type Options = {
		cachePointers: boolean;
		destructive: boolean;
		deterministic: boolean;
		root: boolean;
		ignore: boolean;
		localDependencies: boolean;
		lock?: "auto" | "attr" | "file" | undefined;
		locked: boolean;
		solve: boolean;
		unsolvedDependencies: boolean;
		ttl?: number;
		watch: boolean;
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
	let body = Body.json(Checkin.Arg.toJson(arg));
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
