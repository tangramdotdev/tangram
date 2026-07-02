import * as tg from "../../index.ts";
import { Body, Request, Response } from "../../http.ts";
import type { Client } from "../../client.ts";

export namespace Spawn {
	export type Arg = {
		cached?: boolean;
		cacheLocation?: tg.Location.Arg;
		checksum?: tg.Checksum;
		command: tg.Referent<tg.Command.Id>;
		debug?: tg.Process.Debug;
		location?: tg.Location.Arg;
		parent?: tg.Process.Id;
		public: boolean;
		retry: boolean;
		sandbox?: tg.Sandbox.DataArg | string;
		stderr: tg.Process.Stdio;
		stdin: tg.Process.Stdio;
		stdout: tg.Process.Stdio;
		tty?: boolean | tg.Process.Tty;
	};

	export namespace Arg {
		export let toJson = (arg: tg.Spawn.Arg): unknown => {
			let output: { [key: string]: unknown } = {};
			if (arg.cached !== undefined) {
				output.cached = arg.cached;
			}
			if (arg.cacheLocation !== undefined) {
				output.cache_location = tg.Location.Arg.toDataString(arg.cacheLocation);
			}
			if (arg.checksum !== undefined) {
				output.checksum = arg.checksum;
			}
			output.command = tg.Referent.toData(arg.command, (id) => id);
			if (arg.debug !== undefined) {
				output.debug = arg.debug;
			}
			if (arg.location !== undefined) {
				output.location = tg.Location.Arg.toDataString(arg.location);
			}
			if (arg.parent !== undefined) {
				output.parent = arg.parent;
			}
			if (arg.public) {
				output.public = arg.public;
			}
			if (arg.retry) {
				output.retry = arg.retry;
			}
			if (arg.sandbox !== undefined) {
				output.sandbox = arg.sandbox;
			}
			if (arg.stderr !== "inherit") {
				output.stderr = arg.stderr;
			}
			if (arg.stdin !== "inherit") {
				output.stdin = arg.stdin;
			}
			if (arg.stdout !== "inherit") {
				output.stdout = arg.stdout;
			}
			if (arg.tty !== undefined) {
				output.tty = arg.tty;
			}
			return output;
		};
	}

	export type Output = {
		cached: boolean;
		lease?: string;
		location?: tg.Location;
		process: tg.Process.Id;
		token?: tg.Grant.Token;
		wait?: tg.Process.Wait.Data;
	};
}

export async function spawnProcess(
	client: Client,
	arg: tg.Spawn.Arg,
): Promise<AsyncIterableIterator<tg.Progress.Event<tg.Spawn.Output>>> {
	let stream = await trySpawnProcess(client, arg);
	return mapSpawnEvents(stream);
}

export async function trySpawnProcess(
	client: Client,
	arg: tg.Spawn.Arg,
): Promise<
	AsyncIterableIterator<tg.Progress.Event<tg.Spawn.Output | undefined>>
> {
	let method = "POST";
	let uri = "/processes/spawn";
	let headers = {
		accept: "text/event-stream",
		"content-type": "application/json",
	};
	let body = Body.json(Spawn.Arg.toJson(arg));
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
	return decodeSpawnEvents(response);
}

function normalizeSpawnOutput(
	output: Omit<tg.Spawn.Output, "location"> & {
		location?: string | tg.Location;
	},
): tg.Spawn.Output {
	let { location, wait, ...rest } = output;
	let result: tg.Spawn.Output = { ...rest };
	if (location !== undefined) {
		result.location =
			typeof location === "string"
				? tg.Location.fromDataString(location)
				: location;
	}
	if (wait !== undefined) {
		result.wait = tg.Process.Wait.Data.fromJson(wait);
	}
	return result;
}

async function* decodeSpawnEvents(
	response: Response,
): AsyncIterableIterator<tg.Progress.Event<tg.Spawn.Output | undefined>> {
	for await (let event of tg.Progress.decode<
		| (Omit<tg.Spawn.Output, "location"> & {
				location?: string | tg.Location;
		  })
		| undefined
	>(response)) {
		if (event.kind === "output" && event.value !== undefined) {
			yield {
				kind: "output",
				value: normalizeSpawnOutput(event.value),
			};
		} else {
			yield event as tg.Progress.Event<tg.Spawn.Output | undefined>;
		}
	}
}

async function* mapSpawnEvents(
	events: AsyncIterable<tg.Progress.Event<tg.Spawn.Output | undefined>>,
): AsyncIterableIterator<tg.Progress.Event<tg.Spawn.Output>> {
	for await (let event of events) {
		if (event.kind === "output") {
			if (event.value === undefined) {
				throw new Error("expected a process");
			}
			yield event as tg.Progress.Event<tg.Spawn.Output>;
		} else {
			yield event;
		}
	}
}
