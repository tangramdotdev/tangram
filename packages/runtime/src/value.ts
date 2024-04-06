import { assert as assert_ } from "./assert.ts";
import { Mutation } from "./mutation.ts";
import { Object_ } from "./object.ts";
import { Path } from "./path.ts";
import { Template } from "./template.ts";

export type Value =
	| undefined
	| boolean
	| number
	| string
	| Array<Value>
	| { [key: string]: Value }
	| Object_
	| Uint8Array
	| Path
	| Mutation
	| Template;

export namespace Value {
	export let is = (value: unknown): value is Value => {
		return (
			value === undefined ||
			typeof value === "boolean" ||
			typeof value === "number" ||
			typeof value === "string" ||
			value instanceof Array ||
			(typeof value === "object" && value !== null) ||
			Object_.is(value) ||
			value instanceof Uint8Array ||
			value instanceof Path ||
			value instanceof Mutation ||
			value instanceof Template
		);
	};

	export let expect = (value: unknown): Value => {
		assert_(is(value));
		return value;
	};

	export let assert = (value: unknown): asserts value is Value => {
		assert_(is(value));
	};
}
