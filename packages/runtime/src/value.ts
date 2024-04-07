import { assert as assert_ } from "./assert.ts";
import type { Blob } from "./blob.ts";
import { Branch } from "./branch.ts";
import { Directory } from "./directory.ts";
import { File } from "./file.ts";
import { Leaf } from "./leaf.ts";
import { Lock } from "./lock.ts";
import { Mutation } from "./mutation.ts";
import { Symlink } from "./symlink.ts";
import { Target } from "./target.ts";
import { Template } from "./template.ts";

export type Value =
	| undefined
	| boolean
	| number
	| string
	| Array<Value>
	| { [key: string]: Value }
	| Uint8Array
	| Mutation
	| Template
	| Blob
	| Directory
	| File
	| Symlink
	| Lock
	| Target;

export namespace Value {
	export let is = (value: unknown): value is Value => {
		return (
			value === undefined ||
			typeof value === "boolean" ||
			typeof value === "number" ||
			typeof value === "string" ||
			value instanceof Array ||
			typeof value === "object" ||
			value instanceof Uint8Array ||
			value instanceof Mutation ||
			value instanceof Template ||
			value instanceof Leaf ||
			value instanceof Branch ||
			value instanceof Directory ||
			value instanceof File ||
			value instanceof Symlink ||
			value instanceof Lock ||
			value instanceof Target
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
