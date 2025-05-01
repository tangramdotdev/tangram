import type * as tg from "./index.ts";

export type Module = {
	kind: Module.Kind;
	referent: tg.Referent<tg.Module.Item>;
};

export namespace Module {
	export type Data = {
		kind: Module.Kind;
		referent: tg.Referent<tg.Object.Id>;
	};

	export type Item = string | tg.Object;

	export type Kind =
		| "js"
		| "ts"
		| "dts"
		| "object"
		| "artifact"
		| "blob"
		| "directory"
		| "file"
		| "symlink"
		| "graph"
		| "command";
}
