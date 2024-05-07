import { assert } from "./assert.ts";

export type Module =
	| { kind: "dts"; value: { path: string } }
	| { kind: "artifact"; value: Artifact }
	| { kind: "directory"; value: Directory }
	| { kind: "file"; value: File }
	| { kind: "symlink"; value: Symlink }
	| { kind: "js"; value: Js }
	| { kind: "ts"; value: Js };

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

export type Js =
	| { kind: "file"; value: string }
	| { kind: "package_artifact"; value: PackageArtifact }
	| { kind: "package_path"; value: PackagePath };

export type PackageArtifact = {
	artifact: string;
	lock: string;
	path: string;
};

export type PackagePath = {
	package_path: string;
	path: string;
};

export namespace Module {
	export let toUrl = (module: Module): string => {
		let data = syscall(
			"encoding_hex_encode",
			syscall("encoding_utf8_encode", syscall("encoding_json_encode", module)),
		);
		if (module.kind === "dts") {
			return `tg://${data}/${module.value.path.slice(2)}`;
		} else if (
			module.kind === "js" ||
			(module.kind === "ts" && module.value.kind)
		) {
			let package_ = module.value.value as PackagePath | PackageArtifact;
			return `tg://${data}/${package_.path.slice(2)}`;
		} else if (
			module.kind === "artifact" ||
			module.kind === "directory" ||
			module.kind === "file" ||
			(module.kind === "symlink" && module.value.kind === "path")
		) {
			return `tg://${data}/${module.value.value.slice(2)}`;
		} else {
			return `tg://${data}`;
		}
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
