import { assert } from "./assert.ts";

export type Module =
	| { kind: "js"; value: Js }
	| { kind: "ts"; value: Js }
	| { kind: "dts"; value: { path: string } }
	| { kind: "artifact"; value: Artifact }
	| { kind: "directory"; value: Directory }
	| { kind: "file"; value: File }
	| { kind: "symlink"; value: Symlink };

export type Js =
	| { kind: "file"; value: string }
	| { kind: "package_artifact"; value: PackageArtifact }
	| { kind: "package_path"; value: PackagePath };

export type PackageArtifact = {
	artifact: string;
	path: string;
};

export type PackagePath = {
	package_path: string;
	path: string;
};

export type Artifact =
	| { kind: "path"; value: string }
	| { kind: "id"; value: string };

export type Directory =
	| { kind: "path"; value: string }
	| { kind: "id"; value: string };

export type File =
	| { kind: "path"; value: string }
	| { kind: "id"; value: string };

export type Symlink =
	| { kind: "path"; value: string }
	| { kind: "id"; value: string };

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
