import * as tg from "./index.ts";

export type Object =
	| tg.Blob
	| tg.Directory
	| tg.File
	| tg.Symlink
	| tg.Graph
	| tg.Command;

export namespace Object {
	export type Id =
		| tg.Blob.Id
		| tg.Directory.Id
		| tg.File.Id
		| tg.Symlink.Id
		| tg.Graph.Id
		| tg.Command.Id;

	export type Kind =
		| "blob"
		| "directory"
		| "file"
		| "symlink"
		| "graph"
		| "command";

	export type Object =
		| { kind: "blob"; value: tg.Blob.Object }
		| { kind: "directory"; value: tg.Directory.Object }
		| { kind: "file"; value: tg.File.Object }
		| { kind: "symlink"; value: tg.Symlink.Object }
		| { kind: "graph"; value: tg.Graph.Object }
		| { kind: "command"; value: tg.Command.Object };

	export type State<I, O> = {
		id?: I | undefined;
		object?: O | undefined;
	};

	export let withId = (id: tg.Object.Id): tg.Object => {
		let prefix = id.substring(0, 3);
		if (prefix === "blb") {
			return tg.Blob.withId(id);
		} else if (prefix === "dir") {
			return tg.Directory.withId(id);
		} else if (prefix === "fil") {
			return tg.File.withId(id);
		} else if (prefix === "sym") {
			return tg.Symlink.withId(id);
		} else if (prefix === "gph") {
			return tg.Graph.withId(id);
		} else if (prefix === "tgt") {
			return tg.Command.withId(id);
		} else {
			throw new Error(`invalid object id: ${id}`);
		}
	};

	export let is = (value: unknown): value is Object => {
		return (
			value instanceof tg.Blob ||
			value instanceof tg.Directory ||
			value instanceof tg.File ||
			value instanceof tg.Symlink ||
			value instanceof tg.Graph ||
			value instanceof tg.Command
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
