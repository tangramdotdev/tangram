import * as tg from "../index.ts";

export class Uri {
	path: string;
	query?: string;

	constructor(arg: Uri.Arg) {
		if (typeof arg === "string") {
			let index = arg.indexOf("?");
			if (index === -1) {
				this.path = arg;
			} else {
				this.path = arg.slice(0, index);
				this.query = arg.slice(index + 1);
			}
		} else {
			this.path = arg.path;
			if (typeof arg.query === "string") {
				this.query = arg.query;
			} else {
				let params: Array<string> = [];
				for (let [name, value] of Object.entries(arg.query ?? {})) {
					appendQueryParam(params, name, value);
				}
				this.query = params.join("&");
			}
		}
	}

	toString() {
		if (this.query === undefined || this.query === "") {
			return this.path;
		}
		return `${this.path}?${this.query}`;
	}
}

export namespace Uri {
	export type Arg =
		| string
		| {
				path: string;
				query?: Record<string, QueryValue> | string;
		  };

	export type QueryPrimitive = boolean | number | string | null;
	export type QueryValue = QueryArray | QueryObject | QueryPrimitive;

	export interface QueryArray extends Array<QueryValue> {}

	export interface QueryObject {
		[key: string]: QueryValue;
	}
}

function appendQueryParam(
	params: Array<string>,
	name: string,
	value: Uri.QueryValue,
) {
	if (value === null) {
		return;
	}
	if (Array.isArray(value)) {
		for (let index = 0; index < value.length; index++) {
			appendQueryParam(params, `${name}[${index}]`, value[index]!);
		}
	} else if (typeof value === "object") {
		for (let [key, child] of Object.entries(value)) {
			appendQueryParam(params, `${name}[${key}]`, child);
		}
	} else {
		params.push(`${percentEncode(name)}=${percentEncode(value.toString())}`);
	}
}

export function percentEncode(value: string) {
	let bytes = tg.encoding.utf8.encode(value);
	let output = "";
	for (let byte of bytes) {
		if (
			(byte >= 0x41 && byte <= 0x5a) ||
			(byte >= 0x61 && byte <= 0x7a) ||
			(byte >= 0x30 && byte <= 0x39) ||
			byte === 0x2d ||
			byte === 0x2e ||
			byte === 0x5f ||
			byte === 0x7e
		) {
			output += String.fromCharCode(byte);
		} else {
			output += `%${byte.toString(16).toUpperCase().padStart(2, "0")}`;
		}
	}
	return output;
}
