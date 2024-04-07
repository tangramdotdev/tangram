import { assert } from "./assert.ts";

export type Module =
	| { kind: "document"; value: Document }
	| { kind: "library"; value: Library }
	| { kind: "normal"; value: Normal };

export type Document = {
	package: string;
	path: string;
};

export type Library = {
	path: string;
};

export type Normal = {
	lock: string;
	package: string;
	path: string;
};

export namespace Module {
	export let toUrl = (module: Module): string => {
		let data = syscall(
			"encoding_hex_encode",
			syscall("encoding_utf8_encode", syscall("encoding_json_encode", module)),
		);
		return `tg://${data}/${module.value.path}`;
	};

	export let fromUrl = (url: string): Module => {
		let match = url.match(/^tg:\/\/([0-9a-f]+)/);
		assert(match);
		let [_, data] = match;
		assert(data !== undefined);
		return syscall(
			"encoding_json_decode",
			syscall("encoding_utf8_decode", syscall("encoding_hex_decode", data)),
		) as Module;
	};
}
