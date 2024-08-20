/// <reference lib="es2023" />

interface ImportAttributes {
	path?: string;
}

interface ImportMeta {
	url: string;
}

// @ts-ignore
declare let console: {
	/** Write to the log. */
	log: (...args: Array<unknown>) => void;
};

/**
 * Create a Tangram template with a JavaScript tagged template.
 */
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
		| tg.Path
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
		export type Id = string;

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
		export type Id = string;

		export type ArchiveFormat = "tar" | "zip";

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
		get(arg: tg.Path.Arg): Promise<tg.Artifact>;

		/** Try to get the child at the specified path. This method returns `undefined` if the path does not exist. */
		tryGet(arg: tg.Path.Arg): Promise<tg.Artifact | undefined>;

		/** Get an async iterator of this directory's recursive entries. */
		walk(): AsyncIterableIterator<[string, tg.Artifact]>;

		/** Get an async iterator of this directory's entries. */
		[Symbol.asyncIterator](): AsyncIterator<[string, tg.Artifact]>;
	}

	export namespace Directory {
		export type Id = string;

		export type Arg = undefined | Directory | ArgObject;

		type ArgObject = {
			[key: string]:
				| undefined
				| string
				| Uint8Array
				| Blob
				| Artifact
				| ArgObject;
		};
	}

	/** Create a file. */
	export let file: (...args: Args<File.Arg>) => Promise<File>;

	/** A file. */
	export class File {
		/** Get a file with an ID. */
		static withId(id: File.Id): File;

		/** Create a file. */
		static new(...args: Args<File.Arg>): Promise<File>;

		/** Expect that a value is a `tg.File`. */
		static expect(value: unknown): File;

		/** Assert that a value is a `tg.File`. */
		static assert(value: unknown): asserts value is File;

		/** Get this file's ID. */
		id(): Promise<File.Id>;

		/** Get this file's contents. */
		contents(): Promise<Blob>;

		/** Get the size of this file's contents. */
		size(): Promise<number>;

		/** Get this file's contents as a `Uint8Array`. */
		bytes(): Promise<Uint8Array>;

		/** Get this file's contents as a string. This method throws an error if the contents are not valid UTF-8. */
		text(): Promise<string>;

		/** Get this file's dependencies. */
		dependencies(): Promise<File.Dependencies>;

		/** Get this file's executable bit. */
		executable(): Promise<boolean>;
	}

	export namespace File {
		export type Id = string;

		export type Arg = undefined | string | Uint8Array | Blob | File | ArgObject;

		type ArgObject = {
			contents?: Blob.Arg | undefined;
			dependencies?: Dependencies | undefined;
			executable?: boolean | undefined;
		};

		type Dependencies = Array<Object> | { [reference: string]: Object };
	}

	/** Create a symlink. */
	export let symlink: (...args: Args<Symlink.Arg>) => Promise<Symlink>;

	/** A symlink. */
	export class Symlink {
		/** Get a symlink with an ID. */
		static withId(id: Symlink.Id): Symlink;

		/** Create a symlink. */
		static new(...args: Args<Symlink.Arg>): Promise<Symlink>;

		/** Expect that a value is a `tg.Symlink`. */
		static expect(value: unknown): Symlink;

		/** Assert that a value is a `tg.Symlink`. */
		static assert(value: unknown): asserts value is Symlink;

		/** Get this symlink's ID. */
		id(): Promise<Symlink.Id>;

		/** Get this symlink's artifact. */
		artifact(): Promise<Artifact | undefined>;

		/** Get this symlink's path. */
		path(): Promise<Path | undefined>;

		/** Resolve this symlink to the directory or file it refers to, or return undefined if none is found. */
		resolve(): Promise<Directory | File | undefined>;
	}

	export namespace Symlink {
		export type Id = string;

		export type Arg =
			| undefined
			| string
			| Path
			| Artifact
			| Template
			| Symlink
			| ArgObject;

		type ArgObject = {
			artifact?: Artifact | undefined;
			path?: Path | undefined;
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
			entries?: { [name: string]: number | tg.Artifact };
		};

		type FileNodeArg = {
			kind: "file";
			contents: tg.Blob.Arg;
			dependencies?:
				| Array<number | tg.Object>
				| { [reference: string]: number | tg.Object }
				| undefined;
			executable?: boolean;
		};

		type SymlinkNodeArg = {
			kind: "symlink";
			artifact?: tg.Artifact | undefined;
			path?: tg.Path.Arg | undefined;
		};

		type Node = DirectoryNode | FileNode | SymlinkNode;

		type DirectoryNode = {
			kind: "directory";
			entries: { [name: string]: number | tg.Artifact };
		};

		type FileNode = {
			kind: "file";
			contents: tg.Blob;
			dependencies: { [reference: string]: number | tg.Object } | undefined;
			executable: boolean;
		};

		type SymlinkNode = {
			kind: "symlink";
			artifact: tg.Artifact | undefined;
			path: tg.Path | undefined;
		};
	}

	/** Create a target. */
	export function target<
		A extends Array<Value> = Array<Value>,
		R extends Value = Value,
	>(function_: (...args: A) => Unresolved<R>): Target<A, R>;
	export function target<
		A extends Array<Value> = Array<Value>,
		R extends Value = Value,
	>(...args: Args<Target.Arg>): Promise<Target<A, R>>;

	/** The currently building target. */
	export let current: Target;

	/** A target. */
	export interface Target<
		A extends Array<Value> = Array<Value>,
		R extends Value = Value,
	> {
		/** Build this target. */
		// biome-ignore lint/style/useShorthandFunctionType: interface is necessary .
		(...args: { [K in keyof A]: Unresolved<A[K]> }): Promise<R>;
	}

	/** A target. */
	export class Target<
		A extends Array<Value> = Array<Value>,
		R extends Value = Value,
	> extends globalThis.Function {
		/** Get a target with an ID. */
		static withId(id: Target.Id): Target;

		/** Create a target. */
		static new<A extends Array<Value> = Array<Value>, R extends Value = Value>(
			...args: Args<Target.Arg>
		): Promise<Target<A, R>>;

		/** Expect that a value is a `tg.Target`. */
		static expect(value: unknown): Target;

		/** Assert that a value is a `tg.Target`. */
		static assert(value: unknown): asserts value is Target;

		/** Get this target's ID. */
		id(): Promise<Target.Id>;

		/** Get this target's arguments. */
		args(): Promise<Array<Value>>;

		/** Get this target's checksum. */
		checksum(): Promise<Checksum | undefined>;

		/** Get this target's environment. */
		env(): Promise<{ [key: string]: Value }>;

		/** Get this target's executable. */
		executable(): Promise<Graph | undefined>;

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
			| Artifact
			| Template
			| Target
			| ArgObject;

		type ArgObject = {
			/** The target's command line arguments. */
			args?: Array<Value> | undefined;

			/** If a checksum of the target's output is provided, then the target will have access to the network. */
			checksum?: Checksum | undefined;

			/** The target's environment variables. */
			env?: MaybeNestedArray<MaybeMutationMap> | undefined;

			/** The target's executable. */
			executable?: File | undefined;

			/** The system to build the target on. */
			host?: string | undefined;
		};
	}

	/** Create a path. **/
	export let path: (...args: Array<Path.Arg>) => Path;

	/** A path. **/
	export class Path {
		/** Create a path. **/
		// biome-ignore lint/suspicious/noMisleadingInstantiator:
		static new(...args: Array<MaybeNestedArray<Path.Arg>>): Path;

		/** Get this path's components. **/
		components(): Array<Path.Component>;

		/** Push a component on to this path. **/
		push(component: Path.Component): void;

		/** Push a parent component on to this path. **/
		parent(): Path;

		/** Join this path with another path. **/
		join(other: Path.Arg): Path;

		/** Expect that a value is a `Path`. */
		static expect(value: unknown): Path;

		/** Assert that a value is a `Path`. */
		static assert(value: unknown): asserts value is Path;

		/** Normalize this path. **/
		normalize(): Path;

		/** Return true if this path begins with a current component. **/
		isInternal(): boolean;

		/** Return true if this path begins with a parent component. **/
		isExternal(): boolean;

		/** Return true if this path begins with a root component. **/
		isAbsolute(): boolean;

		/** Render this path to a string. **/
		toString(): string;
	}

	export namespace Path {
		export type Arg = undefined | Component | Path;

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
	}

	/** Create a mutation. */
	export function mutation<T extends Value = Value>(
		arg: Unresolved<Mutation.Arg<T>>,
	): Promise<Mutation<T>>;

	export class Mutation<T extends Value = Value> {
		/** Create a mutation. */
		static new<T extends Value = Value>(
			arg: Unresolved<Mutation.Arg<T>>,
		): Promise<Mutation<T>>;

		/** Create an unset mutation. */
		static unset(): Mutation;

		/** Create a set mutation. */
		static set<T extends Value = Value>(
			value: Unresolved<T>,
		): Promise<Mutation<T>>;

		/** Create a set if unset mutation. */
		static setIfUnset<T extends Value = Value>(
			value: Unresolved<T>,
		): Promise<Mutation<T>>;

		/** Create an prepend mutation. */
		static prepend<T extends Value = Value>(
			values: Unresolved<MaybeNestedArray<T>>,
		): Promise<Mutation<Array<T>>>;

		/** Create an append mutation. */
		static append<T extends Value = Value>(
			values: Unresolved<MaybeNestedArray<T>>,
		): Promise<Mutation<Array<T>>>;

		/** Create a prefix mutation. */
		static prefix(
			template: Unresolved<Template.Arg>,
			separator?: string | undefined,
		): Promise<Mutation<Template>>;

		/** Create a suffix mutation. */
		static suffix(
			template: Unresolved<Template.Arg>,
			separator?: string | undefined,
		): Promise<Mutation<Template>>;

		static expect(value: unknown): Mutation;

		static assert(value: unknown): asserts value is Mutation;

		get inner(): Mutation.Inner;
	}

	export namespace Mutation {
		export type Arg<T extends Value = Value> =
			| { kind: "unset" }
			| { kind: "set"; value: T }
			| { kind: "set_if_unset"; value: T }
			| {
					kind: "prepend";
					values: T extends Array<infer U> ? MaybeNestedArray<U> : never;
			  }
			| {
					kind: "append";
					values: T extends Array<infer U> ? MaybeNestedArray<U> : never;
			  }
			| {
					kind: "prefix";
					template: T extends Template ? Template.Arg : never;
					separator?: string | undefined;
			  }
			| {
					kind: "suffix";
					template: T extends Template ? Template.Arg : never;
					separator?: string | undefined;
			  };

		export type Inner =
			| { kind: "unset" }
			| { kind: "set"; value: Value }
			| { kind: "set_if_unset"; value: Value }
			| {
					kind: "prepend";
					values: Array<Value>;
			  }
			| {
					kind: "append";
					values: Array<Value>;
			  }
			| {
					kind: "prefix";
					template: Template;
					separator: string | undefined;
			  }
			| {
					kind: "suffix";
					template: Template;
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
	export let template: (...args: Args<Template.Arg>) => Promise<Template>;

	/** A template. */
	export class Template {
		static new(...args: Args<Template.Arg>): Promise<Template>;

		/** Expect that a value is a `tg.Template`. */
		static expect(value: unknown): Template;

		/** Assert that a value is a `tg.Template`. */
		static assert(value: unknown): asserts value is Template;

		/** Join an array of templates with a separator. */
		static join(
			separator: Template.Arg,
			...args: Args<Template.Arg>
		): Promise<Template>;

		/** Get this template's components. */
		get components(): Array<Template.Component>;
	}

	export namespace Template {
		export type Arg = undefined | Component | Template;

		export type Component = string | Artifact;
	}

	type Args<T extends Value = Value> = Array<
		Unresolved<MaybeNestedArray<ValueOrMaybeMutationMap<T>>>
	>;

	/* Compute a checksum. */
	export let checksum: (
		input: string | Uint8Array | Blob | Artifact,
		algorithm: Algorithm,
	) => Promise<Checksum>;

	/** A checksum. */
	export type Checksum = string;

	export namespace Checksum {
		export type Algorithm = "blake3" | "sha256" | "sha512" | "unsafe";

		export let new_: (
			input: string | Uint8Array | Blob | Artifact,
			algorithm: Algorithm,
		) => Checksum;
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

	/** Resolve all deeply nested promises in an unresolved value. */
	export let resolve: <T extends Unresolved<Value>>(
		value: T,
	) => Promise<Resolved<T>>;

	/**
	 * This computed type takes a type `T` and returns the union of all possible types that will return `T` by calling `resolve`. Here are some examples:
	 *
	 * ```
	 * Unresolved<string> = MaybePromise<string>
	 * Unresolved<{ key: string }> = MaybePromise<{ key: MaybePromise<string> }>
	 * Unresolved<Array<{ key: string }>> = MaybePromise<Array<MaybePromise<{ key: MaybePromise<string> }>>>
	 * ```
	 */
	export type Unresolved<T extends Value> = MaybePromise<
		T extends
			| undefined
			| boolean
			| number
			| string
			| Object
			| Uint8Array
			| Path
			| Mutation
			| Template
			? T
			: T extends Array<infer U extends Value>
				? Array<Unresolved<U>>
				: T extends { [key: string]: Value }
					? { [K in keyof T]: Unresolved<T[K]> }
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
	export type Resolved<T extends Unresolved<Value>> = T extends
		| undefined
		| boolean
		| number
		| string
		| Object
		| Uint8Array
		| Path
		| Mutation
		| Template
		? T
		: T extends Array<infer U extends Unresolved<Value>>
			? Array<Resolved<U>>
			: T extends { [key: string]: Unresolved<Value> }
				? { [K in keyof T]: Resolved<T[K]> }
				: T extends Promise<infer U extends Unresolved<Value>>
					? Resolved<U>
					: never;

	/** Sleep for the specified duration in seconds. */
	export let sleep: (duration: number) => Promise<void>;

	type MaybeNestedArray<T> = T | Array<MaybeNestedArray<T>>;

	type MaybePromise<T> = T | Promise<T>;

	type MaybeMutation<T extends Value = Value> = T | Mutation<T>;

	type MutationMap<
		T extends { [key: string]: Value } = { [key: string]: Value },
	> = {
		[K in keyof T]?: Mutation<T[K]>;
	};

	type MaybeMutationMap<
		T extends { [key: string]: Value } = { [key: string]: Value },
	> = {
		[K in keyof T]?: MaybeMutation<T[K]>;
	};

	type ValueOrMaybeMutationMap<T extends Value = Value> = T extends
		| undefined
		| boolean
		| number
		| string
		| Uint8Array
		| Blob
		| Directory
		| File
		| Symlink
		| Graph
		| Target
		| Mutation
		| Template
		| Array<infer _U extends Value>
		? T
		: T extends { [key: string]: Value }
			? MaybeMutationMap<T>
			: never;
}
