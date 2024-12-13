/// <reference lib="es2023" />

interface ImportAttributes {
	path?: string;
	remote?: string;
	subpath?: string;
}

interface ImportMeta {
	module: tg.Module;
}

// @ts-ignore
declare let console: {
	/** Write to the log. */
	log: (...args: Array<unknown>) => void;
};

declare function tg(...args: tg.Args<tg.Template.Arg>): Promise<tg.Template>;
declare function tg(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<tg.Template.Arg>
): Promise<tg.Template>;

declare namespace tg {
	/** The union of all types that can be used as the input or output of Tangram targets. */
	export type Value =
		| undefined
		| boolean
		| number
		| string
		| Array<tg.Value>
		| { [key: string]: tg.Value }
		| tg.Object
		| Uint8Array
		| tg.Mutation
		| tg.Template;

	export namespace Value {
		export type Id = string;

		/** Get a value with an ID. */
		export let withId: (id: tg.Value.Id) => tg.Value;

		/** Check if a value is a `tg.Value`. */
		export let is: (value: unknown) => value is tg.Value;

		/** Expect that a value is a `tg.Value`. */
		export let expect: (value: unknown) => tg.Value;

		/** Assert that a value is a `tg.Value`. */
		export let assert: (value: unknown) => asserts value is tg.Value;
	}

	export type Object =
		| tg.Leaf
		| tg.Branch
		| tg.Directory
		| tg.File
		| tg.Symlink
		| tg.Graph
		| tg.Target;

	export namespace Object {
		export type Id =
			| tg.Leaf.Id
			| tg.Branch.Id
			| tg.Directory.Id
			| tg.File.Id
			| tg.Symlink.Id
			| tg.Graph.Id
			| tg.Target.Id;

		/** Get an object with an ID. */
		export let withId: (id: tg.Object.Id) => tg.Object;

		/** Check if a value is an `Object`. */
		export let is: (value: unknown) => value is tg.Object;

		/** Expect that a value is an `Object`. */
		export let expect: (value: unknown) => tg.Object;

		/** Assert that a value is an `Object`. */
		export let assert: (value: unknown) => asserts value is tg.Object;
	}

	/** Create a blob. */
	export let blob: (...args: tg.Args<tg.Blob.Arg>) => Promise<tg.Blob>;

	/** Compress a blob. **/
	export let compress: (
		blob: tg.Blob,
		format: tg.Blob.CompressionFormat,
	) => Promise<tg.Blob>;

	/** Decompress a blob. **/
	export let decompress: (
		blob: tg.Blob,
		format: tg.Blob.CompressionFormat,
	) => Promise<tg.Blob>;

	/** Download the contents of a URL. */
	export let download: (url: string, checksum: tg.Checksum) => Promise<tg.Blob>;

	/** A blob. */
	export type Blob = tg.Leaf | tg.Branch;

	export namespace Blob {
		export type Id = tg.Leaf.Id | tg.Branch.Id;

		export type Arg = undefined | string | Uint8Array | tg.Blob;

		export type CompressionFormat = "bz2" | "gz" | "xz" | "zst";

		/** Check if a value is a `Blob`. */
		export let is: (value: unknown) => value is tg.Artifact;

		/** Expect that a value is a `Blob`. */
		export let expect: (value: unknown) => tg.Artifact;

		/** Assert that a value is a `Blob`. */
		export let assert: (value: unknown) => asserts value is tg.Artifact;

		/** Compress a blob. **/
		export let compress: (
			blob: tg.Blob,
			format: tg.Blob.CompressionFormat,
		) => Promise<tg.Blob>;

		/** Decompress a blob. **/
		export let decompress: (
			blob: tg.Blob,
			format: tg.Blob.CompressionFormat,
		) => Promise<tg.Blob>;

		/** Download a blob. **/
		export let download: (
			url: string,
			checksum: tg.Checksum,
		) => Promise<tg.Blob>;

		/** Checksum a blob. **/
		export let checksum: (
			blob: tg.Blob,
			algorithm: tg.Checksum.Algorithm,
		) => Promise<tg.Checksum>;
	}

	/** Create a leaf. */
	export let leaf: (...args: tg.Args<tg.Leaf.Arg>) => Promise<tg.Leaf>;

	export class Leaf {
		/** Get a leaf with an ID. */
		static withId(id: tg.Leaf.Id): tg.Leaf;

		/** Create a leaf. */
		static new(...args: tg.Args<tg.Leaf.Arg>): Promise<tg.Leaf>;

		/** Expect that a value is a `tg.Leaf`. */
		static expect(value: unknown): tg.Leaf;

		/** Assert that a value is a `tg.Leaf`. */
		static assert(value: unknown): asserts value is tg.Leaf;

		/** Get this leaf's ID. */
		id(): Promise<tg.Leaf.Id>;

		/** Get this leaf's size. */
		size(): Promise<number>;

		/** Get this leaf as a `Uint8Array`. */
		bytes(): Promise<Uint8Array>;

		/** Get this leaf as a string. */
		text(): Promise<string>;
	}

	export namespace Leaf {
		export type Id = string;

		export type Arg = undefined | string | Uint8Array | tg.Leaf;
	}

	/** Create a branch. */
	export let branch: (...args: tg.Args<tg.Branch.Arg>) => Promise<tg.Branch>;

	/** A branch. */
	export class Branch {
		/** Get a branch with an ID. */
		static withId(id: tg.Branch.Id): tg.Branch;

		/** Create a branch. */
		static new(...args: tg.Args<tg.Branch.Arg>): Promise<tg.Branch>;

		/** Expect that a value is a `tg.Branch`. */
		static expect(value: unknown): tg.Branch;

		/** Assert that a value is a `tg.Branch`. */
		static assert(value: unknown): asserts value is tg.Branch;

		/** Get this branch's ID. */
		id(): Promise<tg.Branch.Id>;

		children(): Promise<Array<tg.Branch.Child>>;

		/** Get this branch's size. */
		size(): Promise<number>;

		/** Get this branch as a `Uint8Array`. */
		bytes(): Promise<Uint8Array>;

		/** Get this branch as a string. */
		text(): Promise<string>;
	}

	export namespace Branch {
		export type Id = string;

		export type Arg = undefined | tg.Branch | ArgObject;

		type ArgObject = {
			children?: Array<Child> | undefined;
		};

		export type Child = { blob: Blob; size: number };
	}

	/** An artifact. */
	export type Artifact = tg.Directory | tg.File | tg.Symlink;

	/** Archive an artifact. **/
	export let archive: (
		artifact: tg.Artifact,
		format: tg.Artifact.ArchiveFormat,
	) => Promise<tg.Blob>;

	/** Extract an artifact from an archive. **/
	export let extract: (
		blob: tg.Blob,
		format: tg.Artifact.ArchiveFormat,
	) => Promise<tg.Artifact>;

	/** Bundle an artifact. **/
	export let bundle: (artifact: tg.Artifact) => Promise<tg.Artifact>;

	export namespace Artifact {
		/** An artifact ID. */
		export type Id = tg.Directory.Id | tg.File.Id | tg.Symlink.Id;

		export type ArchiveFormat = "tar" | "tgar" | "zip";

		/** Get an artifact with an ID. */
		export let withId: (id: tg.Artifact.Id) => tg.Artifact;

		/** Check if a value is an `Artifact`. */
		export let is: (value: unknown) => value is tg.Artifact;

		/** Expect that a value is an `Artifact`. */
		export let expect: (value: unknown) => tg.Artifact;

		/** Assert that a value is an `Artifact`. */
		export let assert: (value: unknown) => asserts value is tg.Artifact;

		/** Archive an artifact. **/
		export let archive: (
			artifact: tg.Artifact,
			format: tg.Artifact.ArchiveFormat,
		) => Promise<tg.Blob>;

		/** Extract an artifact from an archive. **/
		export let extract: (
			blob: tg.Blob,
			format: tg.Artifact.ArchiveFormat,
		) => Promise<tg.Artifact>;

		/** Bundle an artifact. **/
		export let bundle: (artifact: tg.Artifact) => Promise<tg.Artifact>;

		/** Checksum an artifact. **/
		export let checksum: (
			artifact: tg.Artifact,
			algorithm: tg.Checksum.Algorithm,
		) => Promise<tg.Checksum>;
	}

	/** Create a directory. */
	export let directory: (
		...args: Array<tg.Unresolved<tg.MaybeNestedArray<tg.Directory.Arg>>>
	) => Promise<tg.Directory>;

	/** A directory. */
	export class Directory {
		/** Get a directory with an ID. */
		static withId(id: tg.Directory.Id): tg.Directory;

		/** Create a directory. */
		static new(
			...args: Array<tg.Unresolved<tg.MaybeNestedArray<tg.Directory.Arg>>>
		): Promise<tg.Directory>;

		/** Expect that a value is a `tg.Directory`. */
		static expect(value: unknown): tg.Directory;

		/** Assert that a value is a `tg.Directory`. */
		static assert(value: unknown): asserts value is tg.Directory;

		/** Get this directory's ID. */
		id(): Promise<tg.Directory.Id>;

		/** Get this directory's entries. */
		entries(): Promise<{ [key: string]: tg.Artifact }>;

		/** Get the child at the specified path. This method throws an error if the path does not exist. */
		get(arg: string): Promise<tg.Artifact>;

		/** Try to get the child at the specified path. This method returns `undefined` if the path does not exist. */
		tryGet(arg: string): Promise<tg.Artifact | undefined>;

		/** Get an async iterator of this directory's recursive entries. */
		walk(): AsyncIterableIterator<[string, tg.Artifact]>;

		/** Get an async iterator of this directory's entries. */
		[Symbol.asyncIterator](): AsyncIterator<[string, tg.Artifact]>;
	}

	export namespace Directory {
		export type Id = string;

		export type Arg = undefined | tg.Directory | ArgObject;

		type ArgObject =
			| {
					[key: string]:
						| undefined
						| string
						| Uint8Array
						| tg.Blob
						| tg.Artifact
						| ArgObject;
			  }
			| { graph: tg.Graph; node: number };
	}

	/** Create a file. */
	export let file: (...args: tg.Args<tg.File.Arg>) => Promise<tg.File>;

	/** A file. */
	export class File {
		/** Get a file with an ID. */
		static withId(id: tg.File.Id): tg.File;

		/** Create a file. */
		static new(...args: tg.Args<tg.File.Arg>): Promise<tg.File>;

		/** Expect that a value is a `tg.File`. */
		static expect(value: unknown): tg.File;

		/** Assert that a value is a `tg.File`. */
		static assert(value: unknown): asserts value is tg.File;

		/** Get this file's ID. */
		id(): Promise<tg.File.Id>;

		/** Get this file's contents. */
		contents(): Promise<tg.Blob>;

		/** Get the size of this file's contents. */
		size(): Promise<number>;

		/** Get this file's contents as a `Uint8Array`. */
		bytes(): Promise<Uint8Array>;

		/** Get this file's contents as a string. This method throws an error if the contents are not valid UTF-8. */
		text(): Promise<string>;

		/** Get this file's dependencies. */
		dependencies(): Promise<
			{ [reference: tg.Reference]: tg.Referent<tg.Object> } | undefined
		>;

		/** Get this file's dependencies as an array. */
		dependencyObjects(): Promise<Array<tg.Object>>;

		/** Get this file's executable bit. */
		executable(): Promise<boolean>;
	}

	export namespace File {
		export type Id = string;

		export type Arg =
			| undefined
			| string
			| Uint8Array
			| tg.Blob
			| tg.File
			| ArgObject;

		type ArgObject =
			| {
					contents?: tg.Blob.Arg | undefined;
					dependencies?:
						| { [reference: tg.Reference]: tg.Referent<tg.Object> }
						| undefined;
					executable?: boolean | undefined;
			  }
			| { graph: tg.Graph; node: number };
	}

	/** Create a symlink. */
	export let symlink: (
		arg: tg.Unresolved<tg.Symlink.Arg>,
	) => Promise<tg.Symlink>;

	/** A symlink. */
	export class Symlink {
		/** Get a symlink with an ID. */
		static withId(id: tg.Symlink.Id): tg.Symlink;

		/** Create a symlink. */
		static new(arg: tg.Unresolved<tg.Symlink.Arg>): Promise<tg.Symlink>;

		/** Expect that a value is a `tg.Symlink`. */
		static expect(value: unknown): tg.Symlink;

		/** Assert that a value is a `tg.Symlink`. */
		static assert(value: unknown): asserts value is tg.Symlink;

		/** Get this symlink's ID. */
		id(): Promise<tg.Symlink.Id>;

		/** Get this symlink's artifact. */
		artifact(): Promise<tg.Artifact | undefined>;

		/** Get this symlink's subpath. */
		subpath(): Promise<string | undefined>;

		/** Resolve this symlink to the artifact it refers to, or return undefined if none is found. */
		resolve(): Promise<tg.Artifact | undefined>;

		/** Get this symlink's target. */
		target(): Promise<string | undefined>;
	}

	export namespace Symlink {
		export type Id = string;

		export type Arg = string | tg.Artifact | tg.Template | Symlink | ArgObject;

		type ArgObject =
			| { graph: tg.Graph; node: number }
			| { target: string }
			| {
					artifact: tg.Artifact;
					subpath?: string | undefined;
			  };
	}

	/** Create a graph. */
	export let graph: (...args: tg.Args<tg.Graph.Arg>) => Promise<tg.Graph>;

	/** A graph. */
	export class Graph {
		/** Get a graph with an ID. */
		static withId(id: tg.Graph.Id): tg.Graph;

		/** Create a graph. */
		static new(...args: tg.Args<tg.Graph.Arg>): Promise<tg.Graph>;

		/** Expect that a value is a `tg.Graph`. */
		static expect(value: unknown): tg.Graph;

		/** Assert that a value is a `tg.Graph`. */
		static assert(value: unknown): asserts value is tg.Graph;

		/** Get this graph's id. */
		id(): Promise<tg.Graph.Id>;

		/** Get this graph's nodes. */
		nodes(): Promise<Array<tg.Graph.Node>>;
	}

	export namespace Graph {
		export type Id = string;

		export type Arg = tg.Graph | ArgObject;

		type ArgObject = {
			nodes?: Array<NodeArg> | undefined;
		};

		type NodeArg = DirectoryNodeArg | FileNodeArg | SymlinkNodeArg;

		type DirectoryNodeArg = {
			kind: "directory";
			entries?: { [name: string]: number | tg.Artifact } | undefined;
		};

		type FileNodeArg = {
			kind: "file";
			contents: tg.Blob.Arg;
			dependencies?:
				| Array<number | tg.Object>
				| { [reference: string]: number | tg.Object }
				| undefined;
			executable?: boolean | undefined;
		};

		type SymlinkNodeArg =
			| {
					kind: "symlink";
					target: string;
			  }
			| {
					kind: "symlink";
					artifact: number | tg.Artifact;
					subpath?: string | undefined;
			  };

		type Node = DirectoryNode | FileNode | SymlinkNode;

		type DirectoryNode = {
			kind: "directory";
			entries: { [name: string]: number | tg.Artifact };
		};

		type FileNode = {
			kind: "file";
			contents: tg.Blob;
			dependencies:
				| { [reference: tg.Reference]: tg.Referent<number | tg.Object> }
				| undefined;
			executable: boolean;
		};

		type SymlinkNode =
			| {
					kind: "symlink";
					target: string;
			  }
			| {
					kind: "symlink";
					artifact: number | tg.Artifact;
					subpath: string | undefined;
			  };
	}

	/** Create a target. */
	export function target<
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	>(function_: (...args: A) => tg.Unresolved<R>): tg.Target<A, R>;
	export function target<
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	>(...args: tg.Args<tg.Target.Arg>): Promise<tg.Target<A, R>>;

	/** A target. */
	export interface Target<
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	> {
		/** Build this target. */
		// biome-ignore lint/style/useShorthandFunctionType: interface is necessary .
		(...args: { [K in keyof A]: tg.Unresolved<A[K]> }): Promise<R>;
	}

	/** A target. */
	export class Target<
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	> extends globalThis.Function {
		/** Get a target with an ID. */
		static withId(id: tg.Target.Id): tg.Target;

		/** Create a target. */
		static new<
			A extends Array<tg.Value> = Array<tg.Value>,
			R extends tg.Value = tg.Value,
		>(...args: tg.Args<tg.Target.Arg>): Promise<tg.Target<A, R>>;

		/** The currently building target. */
		static get current(): tg.Target;

		/** Expect that a value is a `tg.Target`. */
		static expect(value: unknown): tg.Target;

		/** Assert that a value is a `tg.Target`. */
		static assert(value: unknown): asserts value is tg.Target;

		/** Get this target's ID. */
		id(): Promise<tg.Target.Id>;

		/** Get this target's arguments. */
		args(): Promise<Array<tg.Value>>;

		/** Get this target's checksum. */
		checksum(): Promise<tg.Checksum | undefined>;

		/** Get this target's environment. */
		env(): Promise<{ [key: string]: tg.Value }>;

		/** Get this target's executable. */
		executable(): Promise<tg.Target.Executable | undefined>;

		/** Get this target's host. */
		host(): Promise<string>;

		/** Build this target and return the build's output. */
		output(): Promise<R>;
	}

	export namespace Target {
		export type Id = string;

		export type Arg =
			| undefined
			| string
			| tg.Artifact
			| tg.Template
			| tg.Target
			| ArgObject;

		type ArgObject = {
			/** The target's command line arguments. */
			args?: Array<tg.Value> | undefined;

			/** If a checksum of the target's output is provided, then the target will have access to the network. */
			checksum?: tg.Checksum | undefined;

			/** The target's environment variables. */
			env?: tg.MaybeNestedArray<tg.MaybeMutationMap> | undefined;

			/** The target's executable. */
			executable?: tg.Target.ExecutableArg | undefined;

			/** The system to build the target on. */
			host?: string | undefined;
		};

		export type ExecutableArg = tg.Artifact | tg.Module;

		export type Executable = tg.Artifact | tg.Module;
	}

	export namespace path {
		/** A path component. **/
		export type Component =
			| Component.Normal
			| Component.Current
			| Component.Parent
			| Component.Root;

		export namespace Component {
			export type Normal = string;

			export type Current = ".";

			export let Current: string;

			export type Parent = "..";

			export let Parent: string;

			export type Root = "/";

			export let Root: string;

			export let isNormal: (component: Component) => component is Normal;
		}

		/** Split a path into its components */
		export let components: (path: string) => Array<Component>;

		/** Create a path from an array of path components. */
		export let fromComponents: (components: Array<path.Component>) => string;

		/** Return true if the path is absolute.  */
		export let isAbsolute: (path: string) => boolean;

		/** Join paths. */
		export let join: (...paths: Array<string | undefined>) => string;

		/** Return the path with its last component removed. */
		export let parent: (path: string) => string | undefined;
	}

	/** Create a mutation. */
	export function mutation<T extends tg.Value = tg.Value>(
		arg: tg.Unresolved<tg.Mutation.Arg<T>>,
	): Promise<tg.Mutation<T>>;

	export class Mutation<T extends tg.Value = tg.Value> {
		/** Create a mutation. */
		static new<T extends tg.Value = tg.Value>(): Promise<tg.Mutation<T>>;

		/** Create an unset mutation. */
		static unset(): tg.Mutation;

		/** Create a set mutation. */
		static set<T extends tg.Value = tg.Value>(
			value: tg.Unresolved<T>,
		): Promise<tg.Mutation<T>>;

		/** Create a set if unset mutation. */
		static setIfUnset<T extends tg.Value = tg.Value>(
			value: tg.Unresolved<T>,
		): Promise<tg.Mutation<T>>;

		/** Create an prepend mutation. */
		static prepend<T extends tg.Value = tg.Value>(
			values: tg.Unresolved<tg.MaybeNestedArray<T>>,
		): Promise<tg.Mutation<Array<T>>>;

		/** Create an append mutation. */
		static append<T extends tg.Value = tg.Value>(
			values: tg.Unresolved<tg.MaybeNestedArray<T>>,
		): Promise<tg.Mutation<Array<T>>>;

		/** Create a prefix mutation. */
		static prefix(
			template: tg.Unresolved<tg.Template.Arg>,
			separator?: string | undefined,
		): Promise<tg.Mutation<tg.Template>>;

		/** Create a suffix mutation. */
		static suffix(
			template: tg.Unresolved<tg.Template.Arg>,
			separator?: string | undefined,
		): Promise<tg.Mutation<tg.Template>>;

		static expect(value: unknown): tg.Mutation;

		static assert(value: unknown): asserts value is tg.Mutation;

		get inner(): tg.Mutation.Inner;
	}

	export namespace Mutation {
		export type Arg<T extends tg.Value = tg.Value> =
			| { kind: "unset" }
			| { kind: "set"; value: T }
			| { kind: "set_if_unset"; value: T }
			| {
					kind: "prepend";
					values: T extends Array<infer U> ? tg.MaybeNestedArray<U> : never;
			  }
			| {
					kind: "append";
					values: T extends Array<infer U> ? tg.MaybeNestedArray<U> : never;
			  }
			| {
					kind: "prefix";
					template: T extends tg.Template ? tg.Template.Arg : never;
					separator?: string | undefined;
			  }
			| {
					kind: "suffix";
					template: T extends tg.Template ? tg.Template.Arg : never;
					separator?: string | undefined;
			  };

		export type Inner =
			| { kind: "unset" }
			| { kind: "set"; value: tg.Value }
			| { kind: "set_if_unset"; value: tg.Value }
			| {
					kind: "prepend";
					values: Array<tg.Value>;
			  }
			| {
					kind: "append";
					values: Array<tg.Value>;
			  }
			| {
					kind: "prefix";
					template: tg.Template;
					separator: string | undefined;
			  }
			| {
					kind: "suffix";
					template: tg.Template;
					separator: string | undefined;
			  };

		export type Kind =
			| "set"
			| "unset"
			| "set_if_unset"
			| "prepend"
			| "append"
			| "prefix"
			| "suffix";
	}

	/** Create a template. */
	export function template(
		...args: tg.Args<tg.Template.Arg>
	): Promise<tg.Template>;
	export function template(
		strings: TemplateStringsArray,
		...placeholders: tg.Args<tg.Template.Arg>
	): Promise<tg.Template>;

	/** A template. */
	export class Template {
		static new(...args: tg.Args<tg.Template.Arg>): Promise<tg.Template>;

		/** Expect that a value is a `tg.Template`. */
		static expect(value: unknown): tg.Template;

		/** Assert that a value is a `tg.Template`. */
		static assert(value: unknown): asserts value is tg.Template;

		/** Join an array of templates with a separator. */
		static join(
			separator: tg.Template.Arg,
			...args: tg.Args<tg.Template.Arg>
		): Promise<tg.Template>;

		/** Get this template's components. */
		get components(): Array<tg.Template.Component>;
	}

	export namespace Template {
		export type Arg = undefined | Component | tg.Template;

		export type Component = string | tg.Artifact;
	}

	type Args<T extends tg.Value = tg.Value> = Array<
		tg.Unresolved<tg.MaybeNestedArray<tg.ValueOrMaybeMutationMap<T>>>
	>;

	/* Compute a checksum. */
	export let checksum: (
		input: string | Uint8Array | tg.Blob | tg.Artifact,
		algorithm: tg.Checksum.Algorithm,
	) => Promise<tg.Checksum>;

	/** A checksum. */
	export type Checksum = string;

	export namespace Checksum {
		export type Algorithm = "blake3" | "sha256" | "sha512" | "unsafe";

		export let new_: (
			input: string | Uint8Array | tg.Blob | tg.Artifact,
			algorithm: tg.Checksum.Algorithm,
		) => tg.Checksum;
		export { new_ as new };
	}

	/** Assert that a condition is truthy. If not, throw an error with an optional message. */
	export let assert: (
		condition: unknown,
		message?: string,
	) => asserts condition;

	/** Throw an error indicating that unimplemented code has been reached. */
	export let unimplemented: (message?: string) => never;

	/** Throw an error indicating that unreachable code has been reached. */
	export let unreachable: (message?: string) => never;

	export namespace encoding {
		export namespace base64 {
			export let encode: (value: Uint8Array) => string;
			export let decode: (value: string) => Uint8Array;
		}

		export namespace hex {
			export let encode: (value: Uint8Array) => string;
			export let decode: (value: string) => Uint8Array;
		}

		export namespace json {
			export let encode: (value: unknown) => string;
			export let decode: (value: string) => unknown;
		}

		export namespace toml {
			export let encode: (value: unknown) => string;
			export let decode: (value: string) => unknown;
		}

		export namespace utf8 {
			export let encode: (value: string) => Uint8Array;
			export let decode: (value: Uint8Array) => string;
		}

		export namespace yaml {
			export let encode: (value: unknown) => string;
			export let decode: (value: string) => unknown;
		}
	}

	/** Write to the log. */
	export let log: (...args: Array<unknown>) => void;

	export type Module = {
		kind: tg.Module.Kind;
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
			| "target";
	}

	export type Reference = string;

	export type Referent<T> = {
		item: T;
		subpath?: string | undefined;
		tag?: tg.Tag | undefined;
	};

	/** Resolve all deeply nested promises in an unresolved value. */
	export let resolve: <T extends tg.Unresolved<tg.Value>>(
		value: T,
	) => Promise<tg.Resolved<T>>;

	/**
	 * This computed type takes a type `T` and returns the union of all possible types that will return `T` by calling `resolve`. Here are some examples:
	 *
	 * ```
	 * Unresolved<string> = MaybePromise<string>
	 * Unresolved<{ key: string }> = MaybePromise<{ key: MaybePromise<string> }>
	 * Unresolved<Array<{ key: string }>> = MaybePromise<Array<MaybePromise<{ key: MaybePromise<string> }>>>
	 * ```
	 */
	export type Unresolved<T extends tg.Value> = tg.MaybePromise<
		T extends
			| undefined
			| boolean
			| number
			| string
			| tg.Object
			| Uint8Array
			| tg.Mutation
			| tg.Template
			? T
			: T extends Array<infer U extends tg.Value>
				? Array<tg.Unresolved<U>>
				: T extends { [key: string]: tg.Value }
					? { [K in keyof T]: tg.Unresolved<T[K]> }
					: never
	>;

	/**
	 * This computed type performs the inverse of `Unresolved`. It takes a type and returns the output of calling `resolve` on a value of that type. Here are some examples:
	 *
	 * ```
	 * Resolved<string> = string
	 * Resolved<() => string> = string
	 * Resolved<Promise<string>> = string
	 * Resolved<Array<Promise<string>>> = Array<string>
	 * Resolved<() => Promise<Array<Promise<string>>>> = Array<string>
	 * Resolved<Promise<Array<Promise<string>>>> = Array<string>
	 * ```
	 */
	export type Resolved<T extends tg.Unresolved<tg.Value>> = T extends
		| undefined
		| boolean
		| number
		| string
		| tg.Object
		| Uint8Array
		| tg.Mutation
		| tg.Template
		? T
		: T extends Array<infer U extends tg.Unresolved<tg.Value>>
			? Array<Resolved<U>>
			: T extends { [key: string]: tg.Unresolved<tg.Value> }
				? { [K in keyof T]: tg.Resolved<T[K]> }
				: T extends Promise<infer U extends tg.Unresolved<tg.Value>>
					? tg.Resolved<U>
					: never;

	/** Sleep for the specified duration in seconds. */
	export let sleep: (duration: number) => Promise<void>;

	export type Tag = string;

	type MaybeNestedArray<T> = T | Array<tg.MaybeNestedArray<T>>;

	type MaybePromise<T> = T | Promise<T>;

	type MaybeMutation<T extends Value = Value> = T | tg.Mutation<T>;

	type MutationMap<
		T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
	> = {
		[K in keyof T]?: tg.Mutation<T[K]>;
	};

	type MaybeMutationMap<
		T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
	> = {
		[K in keyof T]?: tg.MaybeMutation<T[K]>;
	};

	export type ValueOrMaybeMutationMap<T extends tg.Value = tg.Value> = T extends
		| undefined
		| boolean
		| number
		| string
		| Object
		| Uint8Array
		| tg.Mutation
		| tg.Template
		| Array<infer _U extends tg.Value>
		? T
		: T extends { [key: string]: tg.Value }
			? MaybeMutationMap<T>
			: never;
}
