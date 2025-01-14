import type * as tg from "./index.ts";

export type Module = {
	kind: Module.Kind;
	referent: tg.Referent<tg.Object.Id>;
};

export namespace Module {
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
		| "command";
}
