import * as tg from "./index.ts";

export type Artifact = tg.Directory | tg.File | tg.Symlink;

export namespace Artifact {
	export type Id = string;

	export namespace Id {
		export let is = (value: unknown): value is tg.Artifact.Id => {
			if (typeof value !== "string") {
				return false;
			}
			let prefix = value.substring(0, 3);
			return prefix === "dir" || prefix === "fil" || prefix === "sym";
		};
	}

	export type Kind = "directory" | "file" | "symlink";

	export type Data = tg.Directory.Data | tg.File.Data | tg.Symlink.Data;

	export namespace Data {
		export let is = (value: unknown): value is tg.Artifact.Data => {
			if (tg.Graph.Data.Pointer.is(value)) {
				return true;
			}
			if (
				typeof value !== "object" ||
				value === null ||
				value instanceof Array
			) {
				return false;
			}
			if ("children" in value) {
				return Array.isArray(value.children);
			}
			if ("entries" in value) {
				return (
					value.entries === undefined ||
					(typeof value.entries === "object" &&
						value.entries !== null &&
						!(value.entries instanceof Array))
				);
			}
			if ("contents" in value) {
				return (
					value.contents === undefined || typeof value.contents === "string"
				);
			}
			if ("dependencies" in value) {
				return (
					value.dependencies === undefined ||
					(typeof value.dependencies === "object" &&
						value.dependencies !== null &&
						!(value.dependencies instanceof Array))
				);
			}
			if ("artifact" in value) {
				return (
					value.artifact === undefined ||
					typeof value.artifact === "string" ||
					tg.Graph.Data.Pointer.is(value.artifact)
				);
			}
			if ("path" in value) {
				return value.path === undefined || typeof value.path === "string";
			}
			if ("executable" in value) {
				return (
					value.executable === undefined ||
					typeof value.executable === "boolean"
				);
			}
			if ("module" in value) {
				return value.module === undefined || typeof value.module === "string";
			}
			return globalThis.Object.keys(value).length === 0;
		};
	}

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
