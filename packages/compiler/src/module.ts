export type Module = {
	kind: Module.Kind;
	referent: Referent;
};

type Referent = {
	item: string;
	path?: string;
	tag?: string;
};

export namespace Module {
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
