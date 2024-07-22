import { assert as assert_ } from "./assert.ts";
import { Branch } from "./branch.ts";
import { Directory } from "./directory.ts";
import { File } from "./file.ts";
import { Leaf } from "./leaf.ts";
import { Lock } from "./lock.ts";
import { Symlink } from "./symlink.ts";
import { Target } from "./target.ts";

export type Object_ =
	| Leaf
	| Branch
	| Directory
	| File
	| Symlink
	| Lock
	| Target;

export namespace Object_ {
	export type Id = string;

	export type Kind =
		| "leaf"
		| "branch"
		| "directory"
		| "file"
		| "symlink"
		| "lock"
		| "target";

	export type Object_ =
		| { kind: "leaf"; value: Leaf.Object_ }
		| { kind: "branch"; value: Branch.Object_ }
		| { kind: "directory"; value: Directory.Object_ }
		| { kind: "file"; value: File.Object_ }
		| { kind: "symlink"; value: Symlink.Object_ }
		| { kind: "lock"; value: Lock.Object_ }
		| { kind: "target"; value: Target.Object_ };

	export type State<I, O> = {
		id?: I | undefined;
		object?: O | undefined;
	};

	export let is = (value: unknown): value is Object_ => {
		return (
			value instanceof Leaf ||
			value instanceof Branch ||
			value instanceof Directory ||
			value instanceof File ||
			value instanceof Symlink ||
			value instanceof Lock ||
			value instanceof Target
		);
	};

	export let expect = (value: unknown): Object_ => {
		assert_(is(value));
		return value;
	};

	export let assert = (value: unknown): asserts value is Object_ => {
		assert_(is(value));
	};
}
