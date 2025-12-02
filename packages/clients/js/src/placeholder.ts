import * as tg from "./index.ts";

export function placeholder(name: string): tg.Placeholder {
	return new tg.Placeholder(name);
}

export class Placeholder {
	#name: string;

	constructor(name: string) {
		this.#name = name;
	}

	static expect(value: unknown): tg.Placeholder {
		tg.assert(value instanceof tg.Placeholder);
		return value;
	}

	static assert(value: unknown): asserts value is tg.Placeholder {
		tg.assert(value instanceof tg.Placeholder);
	}

	static toData(value: tg.Placeholder): tg.Placeholder.Data {
		return { name: value.name };
	}

	static fromData(data: tg.Placeholder.Data): tg.Placeholder {
		return new tg.Placeholder(data.name);
	}

	get name(): string {
		return this.#name;
	}
}

export namespace Placeholder {
	export type Data = { name: string };
}

export const output = placeholder("output");
