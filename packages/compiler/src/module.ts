export type Module = {
	kind: Module.Kind;
	referent: Referent;
};

type Referent = {
	item: string;
	path?: string | undefined;
	subpath?: string | undefined;
	tag?: string | undefined;
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
