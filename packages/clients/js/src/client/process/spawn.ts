import * as tg from "../../index.ts";
import { Body, Request, Response } from "../../http.ts";
import type { Client } from "../../client.ts";

export namespace Spawn {
	export type Arg = {
		cache_location?: tg.Location.Arg | undefined;
		checksum?: tg.Checksum | undefined;
		command: tg.Referent<tg.Command.Id>;
		debug?: tg.Process.Debug | undefined;
		location?: tg.Location.Arg | undefined;
		parent?: tg.Process.Id | undefined;
		retry?: boolean | undefined;
		sandbox?: tg.Sandbox.DataArg | string | undefined;
		stderr?: string | undefined;
		stdin?: string | undefined;
		stdout?: string | undefined;
		tty?: boolean | tg.Process.Tty | undefined;
	};

	export type Output = {
		cached: boolean;
		lease: string | undefined;
		location: tg.Location | undefined;
		process: tg.Process.Id;
		token?: tg.Grant.Token | undefined;
		wait: tg.Process.Wait.Data | undefined;
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
	let body = Body.json(normalizeSpawnArg(arg));
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

function normalizeSpawnArg(arg: tg.Spawn.Arg): Omit<
	tg.Spawn.Arg,
	"cache_location" | "command" | "location"
> & {
	cache_location?: string | undefined;
	command: tg.Referent.Data<tg.Command.Id>;
	location?: string | undefined;
} {
	return {
		...arg,
		cache_location:
			arg.cache_location === undefined
				? undefined
				: tg.Location.Arg.toDataString(arg.cache_location),
		command: tg.Referent.toData(arg.command, (id) => id),
		location:
			arg.location === undefined
				? undefined
				: tg.Location.Arg.toDataString(arg.location),
	};
}

function normalizeSpawnOutput(
	output: Omit<tg.Spawn.Output, "location"> & {
		location?: string | tg.Location | undefined;
	},
): tg.Spawn.Output {
	return {
		...output,
		location:
			typeof output.location === "string"
				? tg.Location.fromDataString(output.location)
				: output.location,
		wait:
			output.wait !== undefined
				? tg.Process.Wait.Data.fromJson(output.wait)
				: undefined,
	};
}

async function* decodeSpawnEvents(
	response: Response,
): AsyncIterableIterator<tg.Progress.Event<tg.Spawn.Output | undefined>> {
	for await (let event of tg.Progress.decode<
		| (Omit<tg.Spawn.Output, "location"> & {
				location?: string | tg.Location | undefined;
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
