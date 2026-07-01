import * as tg from "./index.ts";
import { Response } from "./http.ts";

export namespace Progress {
	export type Event<T> =
		| { kind: "diagnostic"; value: tg.Diagnostic.Data }
		| { kind: "indicators"; value: Array<Indicator> }
		| { kind: "log"; value: Log }
		| { kind: "output"; value: T };

	export type Indicator = {
		current?: number | undefined;
		format: IndicatorFormat;
		name: string;
		title: string;
		total?: number | undefined;
	};

	export type IndicatorFormat = "normal" | "bytes";

	export type Log = {
		level?: Level | undefined;
		message: string;
	};

	export type Level = "success" | "info" | "warning" | "error";

	export async function* decode<T>(
		response: Response,
	): AsyncIterableIterator<Event<T>> {
		for await (let event of response.sse()) {
			if (event.event === "error") {
				let data = JSON.parse(event.data) as tg.Error.Data | tg.Error.Id;
				if (typeof data === "string") {
					throw tg.Error.withId(data);
				} else {
					throw tg.Error.fromData(data);
				}
			} else if (event.event === undefined) {
				yield JSON.parse(event.data) as Event<T>;
			} else if (
				event.event === "diagnostic" ||
				event.event === "indicators" ||
				event.event === "log" ||
				event.event === "output"
			) {
				yield {
					kind: event.event,
					value: JSON.parse(event.data) as Event<T>["value"],
				} as Event<T>;
			} else {
				throw new Error("invalid progress event");
			}
		}
	}

	export async function lastOutput<T>(
		events: AsyncIterable<Progress.Event<T>>,
	): Promise<T | undefined> {
		let output: T | undefined;
		for await (let event of events) {
			if (event.kind === "output") {
				output = event.value;
			}
		}
		return output;
	}
}
