export type Module = {
	kind: Module.Kind;
	referent: Referent;
};

type Referent = {
	item: string;
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
		| "leaf"
		| "branch"
		| "directory"
		| "file"
		| "symlink"
		| "graph"
		| "command";
}
