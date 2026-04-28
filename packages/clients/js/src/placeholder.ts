import * as tg from "./index.ts";

/** Create a placeholder. */
export function placeholder(name: string): tg.Placeholder {
	return new tg.Placeholder(name);
}

/** A placeholder. */
export class Placeholder {
	#name: string;

	constructor(name: string) {
		this.#name = name;
	}

	/** Expect that a value is a `tg.Placeholder`. */
	static expect(value: unknown): tg.Placeholder {
		tg.assert(value instanceof tg.Placeholder);
		return value;
	}

	/** Assert that a value is a `tg.Placeholder`. */
	static assert(value: unknown): asserts value is tg.Placeholder {
		tg.assert(value instanceof tg.Placeholder);
	}

	static toData(value: tg.Placeholder): tg.Placeholder.Data {
		return { name: value.name };
	}

	static fromData(data: tg.Placeholder.Data): tg.Placeholder {
		return new tg.Placeholder(data.name);
	}

	/** Get this placeholder's name. */
	get name(): string {
		return this.#name;
	}
}

export namespace Placeholder {
	export type Data = { name: string };
}

/** The output placeholder. */
export const output = placeholder("output");
