import * as tg from "../index.ts";

export class Body implements AsyncIterable<Uint8Array> {
	#body: AsyncIterable<Uint8Array>;

	constructor(body?: AsyncIterable<string | Uint8Array> | undefined) {
		this.#body = normalize(body ?? empty());
	}

	static bytes(bytes: Uint8Array) {
		return new Body(single(bytes));
	}

	static empty() {
		return new Body();
	}

	static json(value: unknown) {
		return Body.text(stringifyJson(value));
	}

	static sse(events: AsyncIterable<Body.SseEvent>) {
		return new Body(encodeSse(events));
	}

	static text(text: string) {
		return Body.bytes(tg.encoding.utf8.encode(text));
	}

	async collect() {
		let chunks: Array<Uint8Array> = [];
		for await (let chunk of this) {
			chunks.push(chunk);
		}
		return concat(chunks);
	}

	async json<T = unknown>() {
		let bytes = await this.collect();
		let string = tg.encoding.utf8.decode(bytes);
		return parseJson(string) as T;
	}

	sse() {
		return decodeSse(this);
	}

	[Symbol.asyncIterator]() {
		return this.#body[Symbol.asyncIterator]();
	}
}

export namespace Body {
	export type SseEvent = {
		data: string;
		event?: string | undefined;
	};
}

async function* decodeSse(
	body: AsyncIterable<Uint8Array>,
): AsyncIterableIterator<Body.SseEvent> {
	let buffer = "";
	for await (let chunk of body) {
		buffer += tg.encoding.utf8.decode(chunk);
		while (true) {
			let index = buffer.indexOf("\n\n");
			let length = 2;
			if (index === -1) {
				index = buffer.indexOf("\r\n\r\n");
				length = 4;
			}
			if (index === -1) {
				break;
			}
			let block = buffer.slice(0, index);
			buffer = buffer.slice(index + length);
			let event = parseSse(block);
			if (event !== undefined) {
				yield event;
			}
		}
	}
	let event = parseSse(buffer);
	if (event !== undefined) {
		yield event;
	}
}

async function* encodeSse(
	events: AsyncIterable<Body.SseEvent>,
): AsyncIterableIterator<Uint8Array> {
	for await (let event of events) {
		yield tg.encoding.utf8.encode(formatSse(event));
	}
}

async function* empty(): AsyncIterableIterator<Uint8Array> {}

async function* single(value: Uint8Array): AsyncIterableIterator<Uint8Array> {
	yield value;
}

async function* normalize(
	body: AsyncIterable<string | Uint8Array>,
): AsyncIterableIterator<Uint8Array> {
	for await (let chunk of body) {
		if (typeof chunk === "string") {
			yield tg.encoding.utf8.encode(chunk);
		} else {
			yield chunk;
		}
	}
}

function parseSse(block: string) {
	let event: string | undefined;
	let data: Array<string> = [];
	for (let line of block.split(/\r\n|\r|\n/)) {
		if (line === "" || line.startsWith(":")) {
			continue;
		}
		let index = line.indexOf(":");
		let name = index === -1 ? line : line.slice(0, index);
		let value = index === -1 ? "" : line.slice(index + 1);
		if (value.startsWith(" ")) {
			value = value.slice(1);
		}
		if (name === "event") {
			event = value;
		} else if (name === "data") {
			data.push(value);
		}
	}
	if (event === undefined && data.length === 0) {
		return undefined;
	}
	return {
		data: data.join("\n"),
		event,
	};
}

function formatSse(event: Body.SseEvent) {
	let output = "";
	if (event.event !== undefined) {
		output += `event: ${event.event}\n`;
	}
	for (let line of event.data.split("\n")) {
		output += `data: ${line}\n`;
	}
	output += "\n";
	return output;
}

function concat(chunks: Array<Uint8Array>) {
	let length = chunks.reduce((length, chunk) => length + chunk.byteLength, 0);
	let output = new Uint8Array(length);
	let offset = 0;
	for (let chunk of chunks) {
		output.set(chunk, offset);
		offset += chunk.byteLength;
	}
	return output;
}

export let stringifyJson = (value: unknown): string =>
	JSON.stringify(undefinedToNull(value));

export let parseJson = (string: string): unknown =>
	nullToUndefined(JSON.parse(string));

let undefinedToNull = (value: unknown): unknown => {
	if (value === undefined) {
		return null;
	} else if (Array.isArray(value)) {
		return value.map(undefinedToNull);
	} else if (isPlainObject(value)) {
		let output: { [key: string]: unknown } = {};
		for (let [key, entry] of Object.entries(value)) {
			output[key] = undefinedToNull(entry);
		}
		return output;
	} else {
		return value;
	}
};

let nullToUndefined = (value: unknown): unknown => {
	if (value === null) {
		return undefined;
	} else if (Array.isArray(value)) {
		return value.map(nullToUndefined);
	} else if (isPlainObject(value)) {
		let output: { [key: string]: unknown } = {};
		for (let [key, entry] of Object.entries(value)) {
			output[key] = nullToUndefined(entry);
		}
		return output;
	} else {
		return value;
	}
};

let isPlainObject = (value: unknown): value is { [key: string]: unknown } => {
	if (typeof value !== "object" || value === null) {
		return false;
	}
	let prototype = Object.getPrototypeOf(value);
	return prototype === Object.prototype || prototype === null;
};
