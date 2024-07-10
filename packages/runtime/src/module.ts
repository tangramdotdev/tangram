import { assert } from "./assert.ts";

export type Module = {
	kind: Kind;
	object: string;
};

export type Kind =
	| "js"
	| "ts"
	| "dts"
	| "artifact"
	| "directory"
	| "file"
	| "symlink";

export namespace Module {
	export let toUrl = (module: Module): string => {
		let prefix = "tg://";
		let json = syscall("encoding_json_encode", module);
		let utf8 = syscall("encoding_utf8_encode", json);
		let hex = syscall("encoding_hex_encode", utf8);
		return `${prefix}${hex}`;
	};

	export let fromUrl = (url: string): Module => {
		let prefix = "tg://";
		assert(url.startsWith(prefix));
		let hex = url.slice(prefix.length);
		let utf8 = syscall("encoding_hex_decode", hex);
		let json = syscall("encoding_utf8_decode", utf8);
		let module = syscall("encoding_json_decode", json) as Module;
		return module;
	};
}
