import * as tg from "./index.ts";

export type Artifact = tg.Directory | tg.File | tg.Symlink;

export namespace Artifact {
	export type Id = string;

	export let withId = (id: Artifact.Id): Artifact => {
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

export let isGraphArg = (
	obj: unknown,
): obj is { graph: tg.Graph; node: number } => {
	return (
		typeof obj === "object" &&
		obj !== null &&
		"graph" in obj &&
		obj.graph instanceof tg.Graph &&
		"node" in obj &&
		typeof obj.node === "number"
	);
};
