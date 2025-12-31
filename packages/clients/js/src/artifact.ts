import * as tg from "./index.ts";

export type Artifact = tg.Directory | tg.File | tg.Symlink;

export namespace Artifact {
	export type Id = string;

	export type Kind = "directory" | "file" | "symlink";

	export let withId = (id: tg.Artifact.Id): tg.Artifact => {
		tg.assert(
			typeof id === "string",
			`expected a string: ${JSON.stringify(id)}`,
		);
		let prefix = id.substring(0, 3);
		if (prefix === "dir") {
			return tg.Directory.withId(id);
		} else if (prefix === "fil") {
			return tg.File.withId(id);
		} else if (prefix === "sym") {
			return tg.Symlink.withId(id);
		} else {
			throw new Error(`invalid artifact id: ${id}`);
		}
	};

	export let withPointer = (pointer: tg.Graph.Pointer): tg.Artifact => {
		switch (pointer.kind) {
			case "directory":
				return tg.Directory.withPointer(pointer);
			case "file":
				return tg.File.withPointer(pointer);
			case "symlink":
				return tg.Symlink.withPointer(pointer);
			default:
				throw new Error(`invalid artifact kind`);
		}
	};

	export let is = (value: unknown): value is tg.Artifact => {
		return (
			value instanceof tg.Directory ||
			value instanceof tg.File ||
			value instanceof tg.Symlink
		);
	};

	export let expect = (value: unknown): tg.Artifact => {
		tg.assert(is(value));
		return value;
	};

	export let assert = (value: unknown): asserts value is tg.Artifact => {
		tg.assert(is(value));
	};
}
