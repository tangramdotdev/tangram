import * as tg from "../../index.ts";
import { Body, Request } from "../../http.ts";
import type { Client } from "../../client.ts";

export namespace Spawn {
	export type Arg = {
		cached?: boolean;
		cacheLocation?: tg.Location.Arg | null;
		checksum?: tg.Checksum | null;
		command: tg.Referent<tg.Command.Id>;
		debug?: tg.Process.Debug | null;
		location?: tg.Location.Arg | null;
		parent?: tg.Process.Id | null;
		public?: boolean;
		retry?: boolean;
		sandbox?: tg.Sandbox.DataArg | string | null;
		stderr?: tg.Process.Stdio;
		stdin?: tg.Process.Stdio;
		stdout?: tg.Process.Stdio;
		tty?: boolean | tg.Process.Tty | null;
	};

	export namespace Arg {
		export let toJson = (arg: tg.Process.Spawn.Arg): unknown => {
			let output: { [key: string]: unknown } = {};
			if (arg.cached !== undefined) {
				output.cached = arg.cached;
			}
			if (arg.cacheLocation !== undefined) {
				output.cache_location =
					arg.cacheLocation === null
						? null
						: tg.Location.Arg.toDataString(arg.cacheLocation);
			}
			if (arg.checksum !== undefined) {
				output.checksum = arg.checksum;
			}
			output.command = tg.Referent.toData(arg.command, (id) => id);
			if (arg.debug !== undefined) {
				output.debug = arg.debug;
			}
			if (arg.location !== undefined) {
				output.location =
					arg.location === null
						? null
						: tg.Location.Arg.toDataString(arg.location);
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
			if (arg.stderr !== undefined && arg.stderr !== "inherit") {
				output.stderr = arg.stderr;
			}
			if (arg.stdin !== undefined && arg.stdin !== "inherit") {
				output.stdin = arg.stdin;
			}
			if (arg.stdout !== undefined && arg.stdout !== "inherit") {
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
		lease?: string | null;
		location?: tg.Location | null;
		process: number | tg.Process.Id;
		token?: tg.Grant.Token | null;
		wait?: tg.Process.Wait.Data | null;
	};

	export namespace Output {
		export let fromJson = (json: unknown): tg.Process.Spawn.Output => {
			let output = json as Omit<tg.Process.Spawn.Output, "location"> & {
				location?: string | tg.Location;
			};
			let { location, wait, ...rest } = output;
			let result: tg.Process.Spawn.Output = { ...rest };
			if (location !== undefined) {
				result.location =
					typeof location === "string"
						? tg.Location.fromDataString(location)
						: location;
			}
			if (wait !== undefined) {
				result.wait = wait;
			}
			return result;
		};
	}
}

export async function spawnProcess(
	client: Client,
	arg: tg.Process.Spawn.Arg,
): Promise<AsyncIterableIterator<tg.Progress.Event<tg.Process.Spawn.Output>>> {
	let stream = await trySpawnProcess(client, arg);
	return mapSpawnEvents(stream);
}

export async function trySpawnProcess(
	client: Client,
	arg: tg.Process.Spawn.Arg,
): Promise<
	AsyncIterableIterator<tg.Progress.Event<tg.Process.Spawn.Output | null>>
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
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	return tg.Progress.decode<tg.Process.Spawn.Output | null>(
		response,
		(output) =>
			output === undefined || output === null
				? null
				: tg.Process.Spawn.Output.fromJson(output),
	);
}

async function* mapSpawnEvents(
	events: AsyncIterable<tg.Progress.Event<tg.Process.Spawn.Output | null>>,
): AsyncIterableIterator<tg.Progress.Event<tg.Process.Spawn.Output>> {
	for await (let event of events) {
		if (event.kind === "output") {
			if (event.value === null) {
				throw new Error("expected a process");
			}
			yield event as tg.Progress.Event<tg.Process.Spawn.Output>;
		} else {
			yield event;
		}
	}
}
