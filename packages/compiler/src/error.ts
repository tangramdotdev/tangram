import type { Module } from "./module.ts";

export class Error_ {
	message: string;
	location: Location | undefined;
	stack: Array<Location> | undefined;
	source: Source | undefined;
	values: Map<string, string> | undefined;

	constructor(
		message: string,
		location?: Location,
		stack?: Array<Location>,
		source?: Source,
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
	file: File;
	line: number;
	column: number;
};

type File =
	| { kind: "internal"; value: string }
	| { kind: "module"; value: Module };

type Source = {
	error: Error_;
	path?: string | undefined;
	tag?: string | undefined;
};
