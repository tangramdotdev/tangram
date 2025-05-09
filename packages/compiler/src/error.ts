import type { Module } from "./module.ts";

export class Error_ {
	message: string;
	location: Location | undefined;
	stack: Array<Location> | undefined;
	source: Error_ | undefined;
	values: Map<string, string> | undefined;

	constructor(
		message: string,
		location?: Location,
		stack?: Array<Location>,
		source?: Error_,
		values?: Map<string, string>,
	) {
		this.message = message;
		this.location = location;
		this.stack = stack;
		this.source = source;
		this.values = values;
	}
}

type Location = {
	symbol?: string;
	source: Source;
	line: number;
	column: number;
};

type Source =
	| { kind: "internal"; value: string }
	| { kind: "external"; value: Module };
