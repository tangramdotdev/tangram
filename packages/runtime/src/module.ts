import { assert } from "./assert.ts";

let scheme = "tg";

export type Module = string;

export type Path = {
	kind: Kind;
	object: string | undefined;
	path: string | undefined;
};

export type Kind =
	| "js"
	| "ts"
	| "dts"
	| "object"
	| "artifact"
	| "blob"
	| "leaf"
	| "branch"
	| "directory"
	| "file"
	| "symlink"
	| "graph"
	| "target";

export type Parsed = { path: Path };

export namespace Module {
	export let print = (parsed: Parsed): Module => {
		let json = syscall("encoding_json_encode", parsed.path);
		let utf8 = syscall("encoding_utf8_encode", json);
		let path = syscall("encoding_hex_encode", utf8);
		return `${scheme}:${path}`;
	};

	export let parse = (module: Module): Parsed => {
		assert(module.startsWith(`${scheme}:`));
		let path_ = module.slice(scheme.length + 1);
		let utf8 = syscall("encoding_hex_decode", path_);
		let json = syscall("encoding_utf8_decode", utf8);
		let path = syscall("encoding_json_decode", json) as Path;
		return { path };
	};
}
