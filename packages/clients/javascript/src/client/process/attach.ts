import * as tg from "../../index.ts";
import { Body, Request, Uri } from "../../http.ts";
import type { Client } from "../../client.ts";

export namespace Attach {
	export type Header = {
		sync?: tg.Referent.Data<string> | undefined;
	};
	export type Mode = "run" | "spawn";
	export type Options = tg.Process.Wait.Arg;
	export type Arg = tg.Process.Wait.Arg & {
		mode: Mode;
		nonce?: tg.Process.Nonce | null;
		open?: Array<tg.Process.Stdio.Stream>;
		process: tg.Process.Spawn.Arg | tg.Process.Id;
	};
	export type ClientRequestArg =
		| { kind: "cancel"; value: tg.Process.Cancel.Arg }
		| { kind: "detach" }
		| { kind: "signal"; value: tg.Signal.Arg }
		| { kind: "tty"; value: tg.Process.Tty.Put.Arg };
	export type ClientMessage =
		| { kind: "ack"; value: { id: number } }
		| { kind: "notification"; value: { kind: "ready" } }
		| { kind: "request"; value: { arg: ClientRequestArg; id: number } };
	export type ServerResponseOutput =
		| { kind: "cancel"; value: tg.Process.Cancel.Output }
		| { kind: "detach" | "signal" | "tty" };
	export type ServerMessage =
		| { kind: "ack"; value: { id: number } }
		| {
				kind: "notification";
				value:
					| {
							kind: "progress";
							value: tg.Progress.Event<tg.Process.Spawn.Output>;
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

export async function attachProcess(
	client: Client,
	arg: Attach.Arg,
	input: AsyncIterable<Attach.ClientMessage>,
): Promise<[Attach.Header, AsyncIterableIterator<Attach.ServerMessage>]> {
	let request = new Request({
		body: Body.sse(encode(input)),
		headers: {
			accept: "text/event-stream",
			"content-type": "text/event-stream",
			...(typeof arg.process === "string"
				? {}
				: { "x-tg-arg-in-body": "true" }),
		},
		method: "POST",
		uri: new Uri({ path: "/processes/attach" }),
	});
	let process = arg.process;
	request.arg(
		locationArg({
			...arg,
			process:
				typeof process === "string"
					? process
					: tg.Process.Spawn.Arg.toJson(process),
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
		throw new Error("invalid process attach content type");
	}
	let header = await response.bodyHeader<Attach.Header>();
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
				throw new Error("invalid process attach message");
			}
			let message = {
				kind: event.event,
				value: JSON.parse(event.data),
			} as Attach.ServerMessage;
			if (
				message.kind === "notification" &&
				message.value.kind === "progress" &&
				message.value.value.kind === "output"
			) {
				message.value.value.value = tg.Process.Spawn.Output.fromJson(
					message.value.value.value,
				);
			}
			yield message;
		}
	})();
	return [header, output];
}

async function* encode(
	input: AsyncIterable<Attach.ClientMessage>,
): AsyncIterableIterator<Body.SseEvent> {
	for await (let message of input) {
		let value: unknown = message.value;
		if (message.kind === "request") {
			let arg = message.value.arg;
			let data: unknown = arg;
			if (
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
