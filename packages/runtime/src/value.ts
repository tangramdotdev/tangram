import * as tg from "./index.ts";

export type Value =
	| undefined
	| boolean
	| number
	| string
	| Array<Value>
	| { [key: string]: Value }
	| tg.Object
	| Uint8Array
	| tg.Path
	| tg.Mutation
	| tg.Template;

export namespace Value {
	export let is = (value: unknown): value is Value => {
		return (
			value === undefined ||
			typeof value === "boolean" ||
			typeof value === "number" ||
			typeof value === "string" ||
			value instanceof Array ||
			(typeof value === "object" && value !== null) ||
			tg.Object.is(value) ||
			value instanceof Uint8Array ||
			value instanceof tg.Path ||
			value instanceof tg.Mutation ||
			value instanceof tg.Template
		);
	};

	export let expect = (value: unknown): Value => {
		tg.assert(is(value));
		return value;
	};

	export let assert = (value: unknown): asserts value is Value => {
		tg.assert(is(value));
	};
}
