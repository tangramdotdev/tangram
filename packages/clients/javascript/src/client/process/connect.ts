import * as tg from "../../index.ts";
import { Body, Request, Uri } from "../../http.ts";
import type { Client } from "../../client.ts";

export namespace Connect {
	export type Header = Record<string, never>;
	export type Mode = "run" | "spawn";
	export type Options = tg.Process.Wait.Arg & {
		reads?: Array<tg.Process.Stdio.Read.Arg>;
	};
	export type Arg = tg.Process.Wait.Arg & {
		mode: Mode;
		process: tg.Process.Spawn.Arg | tg.Process.Id;
		reads: { [id: number]: tg.Process.Stdio.Read.Arg };
	};
	export type ClientRequestArg =
		| { kind: "cancel"; value: tg.Process.Cancel.Arg }
		| { kind: "close"; value: number }
		| { kind: "detach" }
		| { kind: "read"; value: tg.Process.Stdio.Read.Arg }
		| { kind: "signal"; value: tg.Signal.Arg }
		| { kind: "tty"; value: tg.Process.Tty.Put.Arg }
		| { kind: "write"; value: tg.Process.Stdio.Write.Arg };
	export type ClientMessage =
		| { kind: "ack"; value: { id: number } }
		| {
				kind: "notification";
				value:
					| { kind: "ready" }
					| {
							kind: "read_consumption";
							value: {
								id: number;
								consumption: tg.Process.Stdio.Read.Consumption;
							};
					  };
		  }
		| { kind: "request"; value: { arg: ClientRequestArg; id: number } };
	export type ServerResponseOutput =
		| { kind: "cancel"; value: tg.Process.Cancel.Output }
		| { kind: "read"; value: tg.Process.Stdio.Read.Output }
		| { kind: "write"; value: tg.Process.Stdio.Write.Output }
		| { kind: "close" | "detach" | "signal" | "tty" };
	export type ServerMessage =
		| { kind: "ack"; value: { id: number } }
		| {
				kind: "notification";
				value:
					| {
							kind: "progress";
							value: tg.Progress.Event<tg.Process.Spawn.Output>;
					  }
					| {
							kind: "read";
							value: {
								id: number;
								event: tg.Process.Stdio.Read.Event;
							};
					  }
					| { kind: "outcome"; value: tg.Process.Outcome.Data };
		  }
		| {
				kind: "response";
				value: {
					error: tg.Error.Data | null;
					id: number;
					output: ServerResponseOutput | null;
				};
		  };
}

export async function connectProcess(
	client: Client,
	arg: Connect.Arg,
	input: AsyncIterable<Connect.ClientMessage>,
): Promise<[Connect.Header, AsyncIterableIterator<Connect.ServerMessage>]> {
	let request = new Request({
		body: Body.sse(encode(input, client.stdio)),
		headers: {
			accept: "text/event-stream",
			"content-type": "text/event-stream",
			...(typeof arg.process === "string"
				? {}
				: { "x-tg-arg-in-body": "true" }),
		},
		method: "POST",
		uri: new Uri({ path: "/processes/connect" }),
	});
	let process = arg.process;
	request.arg(
		locationArg({
			...arg,
			process:
				typeof process === "string"
					? process
					: tg.Process.Spawn.Arg.toJson(process),
			reads: Object.fromEntries(
				Object.entries(arg.reads).map(([id, read]) => [
					id,
					stdioArg({ ...read, flow: read.flow ?? client.stdio }),
				]),
			),
		}) as Record<string, Uri.QueryValue>,
	);
	let response = await client.send(request);
	if (response.status < 200 || response.status >= 300) {
		throw tg.Error.fromData(await response.json<tg.Error.Data>());
	}
	if (
		response.headers.get("content-type")?.split(";", 1)[0] !==
		"text/event-stream"
	) {
		throw new Error("invalid process connect content type");
	}
	let header = await response.bodyHeader<Connect.Header>();
	let events = response.sse();
	let output = (async function* () {
		for await (let event of events) {
			if (event.event === "error") {
				throw tg.Error.fromData(JSON.parse(event.data));
			}
			if (
				event.event !== "ack" &&
				event.event !== "notification" &&
				event.event !== "response"
			) {
				throw new Error("invalid process connect message");
			}
			let message = {
				kind: event.event,
				value: JSON.parse(event.data),
			} as Connect.ServerMessage;
			if (
				message.kind === "notification" &&
				message.value.kind === "progress" &&
				message.value.value.kind === "output"
			) {
				message.value.value.value = tg.Process.Spawn.Output.fromJson(
					message.value.value.value,
				);
			}
			if (
				message.kind === "response" &&
				message.value.output?.kind === "read"
			) {
				message.value.output.value = tg.Process.Stdio.Read.Output.fromData(
					message.value.output
						.value as unknown as tg.Process.Stdio.Read.Output.Data,
				);
			}
			if (message.kind === "notification" && message.value.kind === "read") {
				let read = message.value.value.event;
				if (read.kind === "chunk") {
					read.value = tg.Process.Stdio.Chunk.fromData(
						read.value as unknown as tg.Process.Stdio.Chunk.Data,
					);
				}
			}
			yield message;
		}
	})();
	return [header, output];
}

async function* encode(
	input: AsyncIterable<Connect.ClientMessage>,
	flow: tg.Process.Stdio.Config,
): AsyncIterableIterator<Body.SseEvent> {
	for await (let message of input) {
		let value: unknown = message.value;
		if (message.kind === "request") {
			let arg = message.value.arg;
			let data: unknown = arg;
			if (arg.kind === "read") {
				data = {
					kind: arg.kind,
					value: stdioArg({ ...arg.value, flow: arg.value.flow ?? flow }),
				};
			} else if (arg.kind === "write") {
				data = {
					kind: "write",
					value: {
						...arg.value,
						data: tg.Process.Stdio.Write.Data.toData(arg.value.data),
						location:
							arg.value.location === null || arg.value.location === undefined
								? null
								: tg.Location.Arg.toDataString(arg.value.location),
					},
				};
			} else if (
				arg.kind === "cancel" ||
				arg.kind === "signal" ||
				arg.kind === "tty"
			) {
				data = { kind: arg.kind, value: locationArg(arg.value) };
			}
			value = { ...message.value, arg: data };
		}
		yield { event: message.kind, data: JSON.stringify(value) };
	}
}

function locationArg<T extends { location?: tg.Location.Arg | null }>(
	arg: T,
): unknown {
	return {
		...arg,
		location:
			arg.location === null || arg.location === undefined
				? null
				: tg.Location.Arg.toDataString(arg.location),
	};
}

function stdioArg(
	arg: tg.Process.Stdio.Read.Arg | tg.Process.Stdio.Write.Stream.Arg,
): unknown {
	return {
		...arg,
		...("flow" in arg
			? { flow: tg.Process.Stdio.Config.toData(arg.flow ?? tg.client.stdio) }
			: {}),
		location:
			arg.location === null || arg.location === undefined
				? null
				: tg.Location.Arg.toDataString(arg.location),
		streams: arg.streams.join(","),
	};
}
