import * as tg from "../../index.ts";
import { Body, Request, Uri } from "../../http.ts";
import type { Client } from "../../client.ts";

export namespace Connect {
	export type Mode = "run" | "spawn";
	export type Options = tg.Process.Wait.Arg & {
		reads?: Array<tg.Process.Stdio.Read.Arg>;
	};
	export type Arg = {
		reads: { [id: number]: tg.Process.Stdio.Read.Arg };
		target:
			| { kind: "existing"; value: tg.Process.Wait.Arg & { id: tg.Process.Id } }
			| { kind: "spawn"; value: { arg: tg.Process.Spawn.Arg; mode: Mode } };
	};
	export type ClientRequestArg =
		| { kind: "cancel"; value: tg.Process.Cancel.Arg }
		| { kind: "close"; value: number }
		| { kind: "connect"; value: Arg }
		| { kind: "detach" }
		| { kind: "read"; value: tg.Process.Stdio.Read.Arg }
		| { kind: "signal"; value: tg.Signal.Arg }
		| { kind: "tty"; value: tg.Process.Tty.Put.Arg }
		| { kind: "write"; value: tg.Process.Stdio.Write.Arg };
	export type ClientMessage =
		| { kind: "ack"; value: { id: number } }
		| {
				kind: "notification";
				value: {
					kind: "read";
					value: {
						id: number;
						progress: tg.Process.Stdio.Read.Progress;
					};
				};
		  }
		| { kind: "request"; value: { arg: ClientRequestArg; id: number } };
	export type ServerResponseOutput =
		| { kind: "cancel"; value: tg.Process.Cancel.Output }
		| { kind: "connect"; value: tg.Process.Spawn.Output }
		| { kind: "read"; value: tg.Process.Stdio.Read.Output }
		| { kind: "write"; value: tg.Process.Stdio.Write.Output }
		| { kind: "close" | "detach" | "signal" | "tty" };
	export type ServerMessage =
		| { kind: "ack"; value: { id: number } }
		| {
				kind: "notification";
				value:
					| { kind: "progress"; value: tg.Progress.Event<null> }
					| {
							kind: "read";
							value: {
								id: number;
								event: tg.Process.Stdio.Read.Event;
							};
					  }
					| { kind: "wait"; value: tg.Process.Wait.Data };
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
	input: AsyncIterable<Connect.ClientMessage>,
): Promise<AsyncIterableIterator<Connect.ServerMessage>> {
	let request = new Request({
		body: Body.sse(encode(input)),
		headers: {
			accept: "text/event-stream",
			"content-type": "text/event-stream",
		},
		method: "POST",
		uri: new Uri({ path: "/processes/connect" }),
	});
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
	return (async function* () {
		for await (let event of response.sse()) {
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
				message.kind === "response" &&
				message.value.output?.kind === "connect"
			) {
				message.value.output.value = tg.Process.Spawn.Output.fromJson(
					message.value.output.value,
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
}

async function* encode(
	input: AsyncIterable<Connect.ClientMessage>,
): AsyncIterableIterator<Body.SseEvent> {
	for await (let message of input) {
		let value: unknown = message.value;
		if (message.kind === "request") {
			let arg = message.value.arg;
			let data: unknown = arg;
			if (arg.kind === "connect") {
				let target = arg.value.target;
				data = {
					kind: arg.kind,
					value: {
						reads: Object.fromEntries(
							Object.entries(arg.value.reads).map(([id, arg]) => [
								id,
								stdioArg(arg),
							]),
						),
						target:
							target.kind === "spawn"
								? {
										kind: "spawn",
										value: {
											arg: tg.Process.Spawn.Arg.toJson(target.value.arg),
											mode: target.value.mode,
										},
									}
								: { kind: "existing", value: locationArg(target.value) },
					},
				};
			} else if (arg.kind === "read") {
				data = { kind: arg.kind, value: stdioArg(arg.value) };
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
		location:
			arg.location === null || arg.location === undefined
				? null
				: tg.Location.Arg.toDataString(arg.location),
		streams: arg.streams.join(","),
	};
}
