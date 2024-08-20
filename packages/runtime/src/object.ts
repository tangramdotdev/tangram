import * as tg from "./index.ts";

export type Object =
	| tg.Leaf
	| tg.Branch
	| tg.Directory
	| tg.File
	| tg.Symlink
	| tg.Graph
	| tg.Target;

export namespace Object {
	export type Id = string;

	export type Kind =
		| "leaf"
		| "branch"
		| "directory"
		| "file"
		| "symlink"
		| "graph"
		| "target";

	export type Object =
		| { kind: "leaf"; value: tg.Leaf.Object }
		| { kind: "branch"; value: tg.Branch.Object }
		| { kind: "directory"; value: tg.Directory.Object }
		| { kind: "file"; value: tg.File.Object }
		| { kind: "symlink"; value: tg.Symlink.Object }
		| { kind: "graph"; value: tg.Graph.Object }
		| { kind: "target"; value: tg.Target.Object };

	export type State<I, O> = {
		id?: I | undefined;
		object?: O | undefined;
	};

	export let is = (value: unknown): value is Object => {
		return (
			value instanceof tg.Leaf ||
			value instanceof tg.Branch ||
			value instanceof tg.Directory ||
			value instanceof tg.File ||
			value instanceof tg.Symlink ||
			value instanceof tg.Graph ||
			value instanceof tg.Target
		);
	};

	export let expect = (value: unknown): Object => {
		tg.assert(is(value));
		return value;
	};

	export let assert = (value: unknown): asserts value is Object => {
		tg.assert(is(value));
	};
}
