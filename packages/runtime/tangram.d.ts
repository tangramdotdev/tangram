/// <reference lib="es2023" />

/**
 * Create a Tangram template with a JavaScript tagged template.
 */
declare function tg(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<tg.Template.Arg>
): Promise<tg.Template>;

declare namespace tg {
	type Args<T extends Value = Value> = Array<
		Unresolved<MaybeNestedArray<ValueOrMaybeMutationMap<T>>>
	>;

	/** An artifact. */
	export type Artifact = Directory | File | Symlink;

	/** Archive an artifact. **/
	export let archive: (
		artifact: Artifact,
		format: Artifact.ArchiveFormat,
	) => Promise<Blob>;

	/** Extract an artifact from an archive. **/
	export let extract: (
		blob: Blob,
		format: Artifact.ArchiveFormat,
	) => Promise<Artifact>;

	/** Bundle an artifact. **/
	export let bundle: (artifact: Artifact) => Promise<Artifact>;

	export namespace Artifact {
		/** An artifact ID. */
		export type Id = string;

		export type ArchiveFormat = "tar" | "zip";

		/** Get an artifact with an ID. */
		export let withId: (id: Artifact.Id) => Artifact;

		/** Check if a value is an `Artifact`. */
		export let is: (value: unknown) => value is Artifact;

		/** Expect that a value is an `Artifact`. */
		export let expect: (value: unknown) => Artifact;

		/** Assert that a value is an `Artifact`. */
		export let assert: (value: unknown) => asserts value is Artifact;

		/** Archive an artifact. **/
		export let archive: (
			artifact: Artifact,
			format: ArchiveFormat,
		) => Promise<Blob>;

		/** Extract an artifact from an archive. **/
		export let extract: (
			blob: Blob,
			format: ArchiveFormat,
		) => Promise<Artifact>;

		/** Bundle an artifact. **/
		export let bundle: (artifact: Artifact) => Promise<Artifact>;

		/** Checksum an artifact. **/
		export let checksum: (
			artifact: Artifact,
			algorithm: Checksum.Algorithm,
		) => Promise<Checksum>;
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

	/** Create a blob. */
	export let blob: (...args: Args<Blob.Arg>) => Promise<Blob>;

	/** Compress a blob. **/
	export let compress: (
		blob: Blob,
		format: Blob.CompressionFormat,
	) => Promise<Blob>;

	/** Decompress a blob. **/
	export let decompress: (
		blob: Blob,
		format: Blob.CompressionFormat,
	) => Promise<Blob>;

	/** Download the contents of a URL. */
	export let download: (url: string, checksum: Checksum) => Promise<Blob>;

	/** A blob. */
	export type Blob = Leaf | Branch;

	export namespace Blob {
		export type Arg = undefined | string | Uint8Array | Blob;

		export type Id = string;

		export type CompressionFormat = "bz2" | "gz" | "xz" | "zst";

		/** Check if a value is a `Blob`. */
		export let is: (value: unknown) => value is Artifact;

		/** Expect that a value is a `Blob`. */
		export let expect: (value: unknown) => Artifact;

		/** Assert that a value is a `Blob`. */
		export let assert: (value: unknown) => asserts value is Artifact;

		/** Compress a blob. **/
		export let compress: (
			blob: Blob,
			format: CompressionFormat,
		) => Promise<Blob>;

		/** Decompress a blob. **/
		export let decompress: (
			blob: Blob,
			format: CompressionFormat,
		) => Promise<Blob>;

		/** Download a blob. **/
		export let download: (url: string, checksum: Checksum) => Promise<Blob>;

		/** Checksum a blob. **/
		export let checksum: (
			blob: Blob,
			algorithm: Checksum.Algorithm,
		) => Promise<Checksum>;
	}

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

	/** Create a directory. */
	export let directory: (
		...args: Array<Unresolved<MaybeNestedArray<Directory.Arg>>>
	) => Promise<Directory>;

	/** A directory. */
	export class Directory {
		/** Get a directory with an ID. */
		static withId(id: Directory.Id): Directory;

		/** Create a directory. */
		static new(
			...args: Array<Unresolved<MaybeNestedArray<Directory.Arg>>>
		): Promise<Directory>;

		/** Expect that a value is a `tg.Directory`. */
		static expect(value: unknown): Directory;

		/** Assert that a value is a `tg.Directory`. */
		static assert(value: unknown): asserts value is Directory;

		/** Get this directory's ID. */
		id(): Promise<Directory.Id>;

		/** Get this directory's entries. */
		entries(): Promise<{ [key: string]: Artifact }>;

		/** Get the child at the specified path. This method throws an error if the path does not exist. */
		get(arg: Path.Arg): Promise<Artifact>;

		/** Try to get the child at the specified path. This method returns `undefined` if the path does not exist. */
		tryGet(arg: Path.Arg): Promise<Artifact | undefined>;

		/** Get an async iterator of this directory's recursive entries. */
		walk(): AsyncIterableIterator<[string, Artifact]>;

		/** Get an async iterator of this directory's entries. */
		[Symbol.asyncIterator](): AsyncIterator<[string, Artifact]>;
	}

	export namespace Directory {
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

		export type Id = string;
	}

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

		/** Get this file's executable bit. */
		executable(): Promise<boolean>;

		/** Get this file's references. */
		references(): Promise<Array<Artifact>>;
	}

	export namespace File {
		export type Arg = undefined | string | Uint8Array | Blob | File | ArgObject;

		type ArgObject = {
			contents?: Blob.Arg | undefined;
			executable?: boolean | undefined;
			references?: Array<Artifact> | undefined;
		};

		export type Id = string;
	}

	/** Create a branch. */
	export let branch: (...args: Args<Branch.Arg>) => Promise<Branch>;

	/** A branch. */
	export class Branch {
		/** Get a branch with an ID. */
		static withId(id: Branch.Id): Branch;

		/** Create a branch. */
		static new(...args: Args<Branch.Arg>): Promise<Branch>;

		/** Expect that a value is a `tg.Branch`. */
		static expect(value: unknown): Branch;

		/** Assert that a value is a `tg.Branch`. */
		static assert(value: unknown): asserts value is Branch;

		/** Get this branch's ID. */
		id(): Promise<Branch.Id>;

		children(): Promise<Array<Branch.Child>>;

		/** Get this branch's size. */
		size(): Promise<number>;

		/** Get this branch as a `Uint8Array`. */
		bytes(): Promise<Uint8Array>;

		/** Get this branch as a string. */
		text(): Promise<string>;
	}

	export namespace Branch {
		export type Arg = undefined | Branch | ArgObject;

		type ArgObject = {
			children?: Array<Child> | undefined;
		};

		export type Child = { blob: Blob; size: number };

		export type Id = string;
	}

	/** Create a leaf. */
	export let leaf: (...args: Args<Leaf.Arg>) => Promise<Leaf>;

	export class Leaf {
		/** Get a leaf with an ID. */
		static withId(id: Leaf.Id): Leaf;

		/** Create a leaf. */
		static new(...args: Args<Leaf.Arg>): Promise<Leaf>;

		/** Expect that a value is a `tg.Leaf`. */
		static expect(value: unknown): Leaf;

		/** Assert that a value is a `tg.Leaf`. */
		static assert(value: unknown): asserts value is Leaf;

		/** Get this leaf's ID. */
		id(): Promise<Leaf.Id>;

		/** Get this leaf's size. */
		size(): Promise<number>;

		/** Get this leaf as a `Uint8Array`. */
		bytes(): Promise<Uint8Array>;

		/** Get this leaf as a string. */
		text(): Promise<string>;
	}

	export namespace Leaf {
		export type Arg = undefined | string | Uint8Array | Leaf;

		export type Id = string;
	}

	/** Write to the log. */
	export let log: (...args: Array<unknown>) => void;

	export type Metadata = {
		homepage?: string;
		license?: string;
		name?: string;
		repository?: string;
		version?: string;
	};

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

	/** Create a lock. */
	export let lock: (...args: Args<Lock.Arg>) => Promise<Lock>;

	/** A lock. */
	export class Lock {
		/** Get a lock with an ID. */
		static withId(id: Lock.Id): Lock;

		/** Create a lock. */
		static new(...args: Args<Lock.Arg>): Promise<Lock>;

		/** Expect that a value is a `tg.Lock`. */
		static expect(value: unknown): Lock;

		/** Assert that a value is a `tg.Lock`. */
		static assert(value: unknown): asserts value is Lock;

		/** Get this lock's dependencies. */
		dependencies(): Promise<{ [key: string]: Lock }>;
	}

	export namespace Lock {
		export type Arg = Lock | ArgObject;

		export type ArgObject = {
			root?: number | undefined;
			nodes?: Array<NodeArg> | undefined;
		};

		export type NodeArg = {
			dependencies?: { [dependency: string]: Lock.Arg | number };
		};

		export type Id = string;
	}

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

		export type Id = string;
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

		/** Get this target's host. */
		host(): Promise<string>;

		/** Get this target's executable. */
		executable(): Promise<Artifact | undefined>;

		/** Get this target's arguments. */
		args(): Promise<Array<Value>>;

		/** Get this target's environment. */
		env(): Promise<{ [key: string]: Value }>;

		/** Get this target's lock. */
		lock(): Promise<string | undefined>;

		/** Get this target's checksum. */
		checksum(): Promise<Checksum | undefined>;

		/** Build this target and return the build's output. */
		output(): Promise<R>;
	}

	export namespace Target {
		export type Arg =
			| undefined
			| string
			| Artifact
			| Template
			| Target
			| ArgObject;

		type ArgObject = {
			/** The system to build the target on. */
			host?: string | undefined;

			/** The target's executable. */
			executable?: Artifact | undefined;

			/** The target's lock. */
			lock?: Lock | undefined;

			/** The target's environment variables. */
			env?: MaybeNestedArray<MaybeMutationMap> | undefined;

			/** The target's command line arguments. */
			args?: Array<Value> | undefined;

			/** If a checksum of the target's output is provided, then the target will have access to the network. */
			checksum?: Checksum | undefined;
		};

		export type Id = string;
	}

	/** The currently building target. */
	export let current: Target;

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
		| Lock
		| Target
		| Mutation
		| Template
		| Array<infer _U extends Value>
		? T
		: T extends { [key: string]: Value }
			? MaybeMutationMap<T>
			: never;

	type MaybeNestedArray<T> = T | Array<MaybeNestedArray<T>>;

	type MaybePromise<T> = T | Promise<T>;

	export type Object =
		| Leaf
		| Branch
		| Directory
		| File
		| Symlink
		| Lock
		| Target;

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

	/** The union of all types that can be used as the input or output of Tangram targets. */
	export type Value =
		| undefined
		| boolean
		| number
		| string
		| Array<Value>
		| { [key: string]: Value }
		| Object
		| Uint8Array
		| Path
		| Mutation
		| Template;

	export namespace Value {
		export type Id = string;

		/** Get a value with an ID. */
		export let withId: (id: Value.Id) => Value;

		/** Check if a value is a `tg.Value`. */
		export let is: (value: unknown) => value is Value;

		/** Expect that a value is a `tg.Value`. */
		export let expect: (value: unknown) => Value;

		/** Assert that a value is a `tg.Value`. */
		export let assert: (value: unknown) => asserts value is Value;
	}
}

// @ts-ignore
declare let console: {
	/** Write to the log. */
	log: (...args: Array<unknown>) => void;
};

interface ImportAttributes {
	id?: string;
	name?: string;
	path?: string;
	version?: string;
}

interface ImportMeta {
	url: string;
}
