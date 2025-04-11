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
	/** Write to stdout. */
	log: (...args: Array<unknown>) => void;

	/** Write to stderr. */
	error: (...args: Array<unknown>) => void;
};

declare function tg(...args: tg.Args<tg.Template.Arg>): Promise<tg.Template>;
declare function tg(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<tg.Template.Arg>
): Promise<tg.Template>;

declare namespace tg {
	/** The union of all types that can be used as the input or output of Tangram commands. */
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

		/** Assert that a value is a valid map. */
		export let isMap: (value: unknown) => value is { [key: string]: tg.Value };
	}

	export type Object =
		| tg.Leaf
		| tg.Branch
		| tg.Directory
		| tg.File
		| tg.Symlink
		| tg.Graph
		| tg.Command;

	export namespace Object {
		export type Id =
			| tg.Leaf.Id
			| tg.Branch.Id
			| tg.Directory.Id
			| tg.File.Id
			| tg.Symlink.Id
			| tg.Graph.Id
			| tg.Command.Id;

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
	export let decompress: (blob: tg.Blob) => Promise<tg.Blob>;

	/** Download the contents of a URL. */
	export let download: (url: string, unpack: boolean, checksum: tg.Checksum) => Promise<tg.Blob | tg.Artifact>;

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
		export let decompress: (blob: tg.Blob) => Promise<tg.Blob>;

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

		/** Get this leaf's length. */
		length(): Promise<number>;

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
		
		/** Combine a set of branch args into a single branch arg object. */
		static arg(...args: tg.Args<tg.Branch.Arg>): Promise<tg.Branch.ArgObject>;

		/** Expect that a value is a `tg.Branch`. */
		static expect(value: unknown): tg.Branch;

		/** Assert that a value is a `tg.Branch`. */
		static assert(value: unknown): asserts value is tg.Branch;

		/** Get this branch's ID. */
		id(): Promise<tg.Branch.Id>;

		children(): Promise<Array<tg.Branch.Child>>;

		/** Get this branch's length. */
		length(): Promise<number>;

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
		compression?: tg.Blob.CompressionFormat,
	) => Promise<tg.Blob>;

	/** Extract an artifact from an archive. **/
	export let extract: (blob: tg.Blob) => Promise<tg.Artifact>;

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
			compression?: tg.Blob.CompressionFormat,
		) => Promise<tg.Blob>;

		/** Download an artifact. **/
		export let download: (
			url: string,
			checksum: tg.Checksum,
		) => Promise<tg.Artifact>;

		/** Extract an artifact from an archive. **/
		export let extract: (blob: tg.Blob) => Promise<tg.Artifact>;

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
		
		/** Combine a set of file args into a single file arg object. */
		static arg(...args: tg.Args<tg.File.Arg>): Promise<tg.File.ArgObject>;

		/** Expect that a value is a `tg.File`. */
		static expect(value: unknown): tg.File;

		/** Assert that a value is a `tg.File`. */
		static assert(value: unknown): asserts value is tg.File;

		/** Get this file's ID. */
		id(): Promise<tg.File.Id>;

		/** Get this file's contents. */
		contents(): Promise<tg.Blob>;

		/** Get the length of this file's contents. */
		length(): Promise<number>;

		/** Get this file's contents as a `Uint8Array`. */
		bytes(): Promise<Uint8Array>;

		/** Get this file's contents as a string. This method throws an error if the contents are not valid UTF-8. */
		text(): Promise<string>;

		/** Get this file's dependencies. */
		dependencies(): Promise<{
			[reference: tg.Reference]: tg.Referent<tg.Object>;
		}>;

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
					contents?: tg.Blob.Arg | Array<tg.Blob.Arg> | undefined;
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
		
		/** Resolve a symlink arg into a symlink arg object. */
		static arg(arg: tg.Unresolved<tg.Symlink.Arg>): Promise<tg.Symlink.ArgObject>;

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
			| { command: string }
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
		
		/** Combine a set of graph args into a single graph arg object. */
		static arg(...args: tg.Args<tg.Graph.Arg>): Promise<tg.Graph.ArgObject>;

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
					command: string;
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
			dependencies: {
				[reference: tg.Reference]: tg.Referent<number | tg.Object>;
			};
			executable: boolean;
		};

		type SymlinkNode =
			| {
					kind: "symlink";
					command: string;
			  }
			| {
					kind: "symlink";
					artifact: number | tg.Artifact;
					subpath: string | undefined;
			  };
	}

	/** Create a command. */
	export function command<
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	>(function_: (...args: A) => tg.Unresolved<R>): tg.Command<A, R>;
	export function command<
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	>(...args: tg.Args<tg.Command.Arg>): Promise<tg.Command<A, R>>;
	export function command<
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	>(
		strings: TemplateStringsArray,
		...placeholders: tg.Args<tg.Template.Arg>
	): CommandBuilder;

	/** A command. */
	export interface Command<
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	> {
		/** Build this command. */
		// biome-ignore lint/style/useShorthandFunctionType: interface is necessary .
		(...args: { [K in keyof A]: tg.Unresolved<A[K]> }): Promise<R>;
	}

	/** A command. */
	export class Command<
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	> extends globalThis.Function {
		/** Get a command with an ID. */
		static withId(id: tg.Command.Id): tg.Command;

		/** Create a command. */
		static new<
			A extends Array<tg.Value> = Array<tg.Value>,
			R extends tg.Value = tg.Value,
		>(...args: tg.Args<tg.Command.Arg>): Promise<tg.Command<A, R>>;

		/** Combine a set of command args into a single command arg object. */
		static arg(...args: tg.Args<tg.Command.Arg>): Promise<tg.Command.ArgObject>;
	
		/** Expect that a value is a `tg.Command`. */
		static expect(value: unknown): tg.Command;

		/** Assert that a value is a `tg.Command`. */
		static assert(value: unknown): asserts value is tg.Command;

		/** Get this command's ID. */
		id(): Promise<tg.Command.Id>;

		/** Get this command's arguments. */
		args(): Promise<Array<tg.Value>>;

		/** Get this command's cwd. */
		cwd(): Promise<string | undefined>;

		/** Get this command's environment. */
		env(): Promise<{ [key: string]: tg.Value }>;

		/** Get this command's executable. */
		executable(): Promise<tg.Command.Executable>;

		/** Get this command's host. */
		host(): Promise<string>;

		/** Get this command's object. */
		object(): Promise<tg.Command.Object>;

		/** Get this command's mounts. */
		mounts(): Promise<Array<tg.Command.Mount> | undefined>;

		/** Get this command's user. */
		user(): Promise<string | undefined>;

		/** Build this command and return the process's output. */
		build(...args: A): Promise<R>;

		/** Run this command and return the process's output. */
		run(...args: A): Promise<R>;
	}

	export namespace Command {
		export type Id = string;

		export type Arg =
			| undefined
			| string
			| tg.Artifact
			| tg.Template
			| tg.Command
			| ArgObject;

		type ArgObject = {
			/** The command's arguments. */
			args?: Array<tg.Value> | undefined;

			/** The command's working directory. **/
			cwd?: string | undefined;

			/** The command's environment. */
			env?: tg.MaybeMutationMap | undefined;

			/** The command's executable. */
			executable?: tg.Command.ExecutableArg | undefined;

			/** The command's host. */
			host?: string | undefined;

			/** The command's mounts. */
			mounts?: Array<string | tg.Template | tg.Command.Mount> | undefined;

			/** The command's user. */
			user?: string | undefined;

			/** The command's stdin. */
			stdin?: tg.Blob.Arg | undefined;
		};

		export type Object = {
			args: Array<tg.Value>;
			cwd: string | undefined;
			env: { [key: string]: tg.Value };
			executable: tg.Command.Executable;
			host: string;
			mounts: Array<tg.Command.Mount> | undefined;
			stdin: tg.Blob | undefined;
			user: string | undefined;
		};

		export type ExecutableArg = string | tg.Artifact | tg.Module;

		export type Executable = string | tg.Artifact | tg.Module;

		/** A mount. */
		export type Mount = {
			source: tg.Artifact;
			target: string;
		};

		export namespace Mount {
			/** Parse a mount. */
			export let parse: (
				arg: string | tg.Template,
			) => Promise<tg.Command.Mount>;
		}
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

		/** Create a merge mutation. */
		static merge(
			value: { [key: string]: tg.Value}
		): Promise<tg.Mutation<{ [key: string]: tg.Value }>>;

		static expect(value: unknown): tg.Mutation;

		static assert(value: unknown): asserts value is tg.Mutation;

		apply(map: { [key: string]: tg.Value }, key: string): Promise<void>;

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
			  }
			| {
					kind: "merge";
					value: T extends { [key: string]: tg.Value } ? T : never;
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
			  }
			| {
					kind: "merge";
					value: { [key: string]: tg.Value }
			};

		export type Kind =
			| "set"
			| "unset"
			| "set_if_unset"
			| "prepend"
			| "append"
			| "prefix"
			| "suffix"
			| "merge";
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

		/** A tagged template function that behaves identically to `tg.template` except that it does not trim leading whitespace. **/
		export let raw: (
			strings: TemplateStringsArray,
			...placeholders: tg.Args<tg.Template.Arg>
		) => Promise<tg.Template>;
	}

	type Args<T extends tg.Value = tg.Value> = Array<
		tg.Unresolved<tg.MaybeNestedArray<tg.ValueOrMaybeMutationMap<T>>>
	>;

	export namespace Args {
		export type Rules<
			T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
		> = {
			[K in keyof T]:
				| tg.Mutation.Kind
				| ((arg: T[K]) => tg.MaybePromise<tg.Mutation<T[K]>>);
		};

		export function apply<
			T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
		>(args: Array<tg.MaybeMutationMap<T>>, rules?: Rules<T>): Promise<T>;
	}

	/* Compute a checksum. */
	export let checksum: (
		input: string | Uint8Array | tg.Blob | tg.Artifact,
		algorithm: tg.Checksum.Algorithm,
	) => Promise<tg.Checksum>;

	/** A checksum. */
	export type Checksum = string;

	export namespace Checksum {
		export type Algorithm = "none" | "unsafe" | "blake3" | "sha256" | "sha512";

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

	/** Write to stdout. */
	export let log: (...args: Array<unknown>) => void;

	/** Write to stderr. */
	export let error: (...args: Array<unknown>) => void;

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
			| "command";
	}

	export function build(
		...args: tg.Args<tg.Process.BuildArg>
	): Promise<tg.Value>;
	export function build(
		strings: TemplateStringsArray,
		...placeholders: tg.Args<tg.Template.Arg>
	): BuildBuilder;

	export function run(...args: tg.Args<tg.Process.RunArg>): Promise<tg.Value>;
	export function run(
		strings: TemplateStringsArray,
		...placeholders: tg.Args<tg.Template.Arg>
	): RunBuilder;

	export let $: typeof run;

	/** The current process. */
	export let process: tg.Process;

	export class Process {
		/** Expect that a value is a `tg.Process`. */
		static expect(value: unknown): tg.Process;

		/** Assert that a value is a `tg.Process`. */
		static assert(value: unknown): asserts value is tg.Process;

		/** Combine a set of build args into a single build arg object. */
		static buildArg(
			...args: tg.Args<tg.Process.BuildArg>
		): Promise<tg.Process.BuildArgObject>;
		
		/** Combine a set of run args into a single build arg object. */
		static runArg(
			...args: tg.Args<tg.Process.RunArg>
		): Promise<tg.Process.RunArgObject>;

		/** Load the process's state. */
		load(): Promise<void>;

		/** Reload the process's state. */
		reload(): Promise<void>;

		/** Get this process's ID. */
		id(): tg.Process.Id;

		/** Get this process's checksum. */
		checksum(): Promise<tg.Checksum | undefined>;

		/** Get this process's command. */
		command(): Promise<tg.Command>;

		/** Get this process's command's args. */
		args(): Promise<Array<tg.Value>>;

		/** Get this process's command's cwd. */
		cwd(): Promise<string | undefined>;

		/** Get this process's command's environment. */
		env(): Promise<{ [name: string]: tg.Value }>;
		env(name: string): Promise<tg.Value | undefined>;

		/** Get this process's command's executable. */
		executable(): Promise<tg.Command.Executable>;

		/** Get the mounts for this process and its command. */
		mounts(): Promise<Array<tg.Command.Mount | tg.Process.Mount>>;

		/** Get whether this process has the network enabled. */
		network(): Promise<boolean>;

		/** Get this process's command's user. */
		user(): Promise<string | undefined>;
	}

	export namespace Process {
		export type Id = string;

		export type BuildArg =
			| undefined
			| string
			| tg.Artifact
			| tg.Template
			| tg.Command
			| BuildArgObject;

		export type BuildArgObject = {
			/** The command's arguments. */
			args?: Array<tg.Value> | undefined;

			/** If a checksum of the process's output is provided, then the process can be cached even if it is not sandboxed. */
			checksum?: tg.Checksum | undefined;

			/** The command's working directory. **/
			cwd?: string | undefined;

			/** The command's environment. */
			env?: tg.MaybeMutationMap | undefined;

			/** The command's executable. */
			executable?: tg.Command.ExecutableArg | undefined;

			/** The command's host. */
			host?: string | undefined;

			/** The command's mounts. */
			mounts?: Array<string | tg.Template | tg.Command.Mount> | undefined;

			/** Configure whether the process has access to the network. **/
			network?: boolean | undefined;

			/** Ignore stdin, or set it to a blob. */
			stdin?: tg.Blob.Arg | undefined;

			/** The command's user. */
			user?: string | undefined;
		};

		export type RunArg =
			| undefined
			| string
			| tg.Artifact
			| tg.Template
			| tg.Command
			| RunArgObject;

		export type RunArgObject = {
			/** The command's arguments. */
			args?: Array<tg.Value> | undefined;

			/** If a checksum of the process's output is provided, then the process can be cached even if it is not sandboxed. */
			checksum?: tg.Checksum | undefined;

			/** The command's working directory. **/
			cwd?: string | undefined;

			/** The command's environment. */
			env?: tg.MaybeMutationMap | undefined;

			/** The command's executable. */
			executable?: tg.Command.ExecutableArg | undefined;

			/** The command's host. */
			host?: string | undefined;

			/** The command's or process's mounts. */
			mounts?:
				| Array<string | tg.Template | tg.Command.Mount | tg.Process.Mount>
				| undefined;

			/** Configure whether the process has access to the network. **/
			network?: boolean | undefined;

			/** Suppress stderr. */
			stderr?: undefined;

			/** Ignore stdin, or set it to a blob. */
			stdin?: tg.Blob.Arg | undefined;

			/** Suppress stdout. */
			stdout?: undefined;

			/** The command's user. */
			user?: string | undefined;
		};

		/** A mount. */
		export type Mount = {
			source: string;
			target: string;
			readonly: boolean;
		};

		export namespace Mount {
			/** Parse a mount. */
			export let parse: (
				arg: string | tg.Template,
			) => Promise<tg.Process.Mount>;
		}
	}
		
	export class BuildBuilder {
		constructor(...args: tg.Args<tg.Process.BuildArgObject>);
		args(args: tg.Unresolved<tg.MaybeMutation<Array<tg.Value>>>): this;
		checksum(
			checksum: tg.Unresolved<tg.MaybeMutation<tg.Checksum | undefined>>,
		): this;
		cwd(cwd: tg.Unresolved<tg.MaybeMutation<string | undefined>>): this;
		env(env: tg.Unresolved<tg.MaybeMutation<tg.MaybeMutationMap>>): this;
		executable(executable: tg.Unresolved<tg.MaybeMutation<tg.Command.ExecutableArg>>): this;
		host(host: tg.Unresolved<tg.MaybeMutation<string>>): this;
		mount(mounts: tg.Unresolved<tg.MaybeMutation<Array<string | tg.Template | tg.Command.Mount>>>): this;
		network(network: tg.Unresolved<tg.MaybeMutation<boolean>>): this;
		then<TResult1 = tg.Value, TResult2 = never>(
			onfulfilled?:
				| ((value: tg.Value) => TResult1 | PromiseLike<TResult1>)
				| undefined
				| null,
			onrejected?:
				| ((reason: any) => TResult2 | PromiseLike<TResult2>)
				| undefined
				| null,
		): PromiseLike<TResult1 | TResult2>;
	}
	
	export class CommandBuilder {
		constructor(...args: tg.Args<tg.Command.ArgObject>);
		args(args: tg.Unresolved<tg.MaybeMutation<Array<tg.Value>>>): this;
		cwd(cwd: tg.Unresolved<tg.MaybeMutation<string | undefined>>): this;
		env(env: tg.Unresolved<tg.MaybeMutation<tg.MaybeMutationMap>>): this;
		executable(executable: tg.Unresolved<tg.MaybeMutation<tg.Command.ExecutableArg>>): this;
		host(host: tg.Unresolved<tg.MaybeMutation<string>>): this;
		mount(mounts: tg.Unresolved<tg.MaybeMutation<Array<string | tg.Template | tg.Command.Mount>>>): this;
		then<TResult1 = tg.Value, TResult2 = never>(
			onfulfilled?:
				| ((value: tg.Value) => TResult1 | PromiseLike<TResult1>)
				| undefined
				| null,
			onrejected?:
				| ((reason: any) => TResult2 | PromiseLike<TResult2>)
				| undefined
				| null,
		): PromiseLike<TResult1 | TResult2>;
	}
	
	export class RunBuilder {
		constructor(...args: tg.Args<tg.Process.RunArgObject>);
		args(args: tg.Unresolved<tg.MaybeMutation<Array<tg.Value>>>): this;
		checksum(
			checksum: tg.Unresolved<tg.MaybeMutation<tg.Checksum | undefined>>,
		): this;
		cwd(cwd: tg.Unresolved<tg.MaybeMutation<string | undefined>>): this;
		env(env: tg.Unresolved<tg.MaybeMutation<tg.MaybeMutationMap>>): this;
		executable(executable: tg.Unresolved<tg.MaybeMutation<tg.Command.ExecutableArg>>): this;
		host(host: tg.Unresolved<tg.MaybeMutation<string>>): this;
		mount(mounts: tg.Unresolved<tg.MaybeMutation<Array<string | tg.Template | tg.Command.Mount | tg.Process.Mount>>>): this;
		network(network: tg.Unresolved<tg.MaybeMutation<boolean>>): this;
		then<TResult1 = tg.Value, TResult2 = never>(
			onfulfilled?:
				| ((value: tg.Value) => TResult1 | PromiseLike<TResult1>)
				| undefined
				| null,
			onrejected?:
				| ((reason: any) => TResult2 | PromiseLike<TResult2>)
				| undefined
				| null,
		): PromiseLike<TResult1 | TResult2>;
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
