import * as tg from "./index.ts";

export type Artifact = tg.Directory | tg.File | tg.Symlink;

export namespace Artifact {
	export type Id = string;

	export type Kind = "directory" | "file" | "symlink";

	export let withId = (id: Artifact.Id): Artifact => {
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

	export let withReference = (reference: tg.Graph.Reference): Artifact => {
		switch (reference.kind) {
			case "directory":
				return tg.Directory.withReference(reference);
			case "file":
				return tg.File.withReference(reference);
			case "symlink":
				return tg.Symlink.withReference(reference);
			default:
				throw new Error(`invalid artifact kind`);
		}
	};

	export let is = (value: unknown): value is Artifact => {
		return (
			value instanceof tg.Directory ||
			value instanceof tg.File ||
			value instanceof tg.Symlink
		);
	};

	export let expect = (value: unknown): Artifact => {
		tg.assert(is(value));
		return value;
	};

	export let assert = (value: unknown): asserts value is Artifact => {
		tg.assert(is(value));
	};
}
