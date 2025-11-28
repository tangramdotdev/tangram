/// <reference lib="es2023" />

declare interface ImportAttributes {
	path?: string;
}

declare interface ImportMeta {
	module: tg.Module;
}

declare let console: {
	/** Write to stdout. */
	log: (...args: Array<unknown>) => void;

	/** Write to stderr. */
	error: (...args: Array<unknown>) => void;
};

declare function tg(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<tg.Template.Arg>
): Promise<tg.Template>;
declare function tg(...args: tg.Args<tg.Template.Arg>): Promise<tg.Template>;

declare namespace tg {
	export type ArchiveFormat = "tar" | "tgar" | "zip";

	export type CompressionFormat = "bz2" | "gz" | "xz" | "zst";

	export type DownloadOptions = {
		checksum?: tg.Checksum.Algorithm | undefined;
		mode?: "raw" | "decompress" | "extract" | undefined;
	};

	/** Archive an artifact. **/
	export let archive: (
		artifact: tg.Artifact,
		format: tg.ArchiveFormat,
		compression?: tg.CompressionFormat,
	) => Promise<tg.Blob>;

	/** Bundle an artifact. **/
	export let bundle: (artifact: tg.Artifact) => Promise<tg.Artifact>;

	/** Compress a blob. **/
	export let compress: (
		blob: tg.Blob,
		format: tg.CompressionFormat,
	) => Promise<tg.Blob>;

	/** Decompress a blob. **/
	export let decompress: (blob: tg.Blob) => Promise<tg.Blob>;

	/** Download an artifact. **/
	export let download: (
		url: string,
		checksum: tg.Checksum,
		options?: tg.DownloadOptions,
	) => Promise<tg.Blob | tg.Artifact>;

	/** Extract an artifact from an archive. **/
	export let extract: (blob: tg.Blob) => Promise<tg.Artifact>;

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
		| tg.Blob
		| tg.Directory
		| tg.File
		| tg.Symlink
		| tg.Graph
		| tg.Command;

	export namespace Object {
		export type Id =
			| tg.Blob.Id
			| tg.Directory.Id
			| tg.File.Id
			| tg.Symlink.Id
			| tg.Graph.Id
			| tg.Command.Id;

		/** Get an object with an ID. */
		export let withId: (id: tg.Object.Id) => tg.Object;

		/** Check if a value is a `tg.Object`. */
		export let is: (value: unknown) => value is tg.Object;

		/** Expect that a value is a `tg.Object`. */
		export let expect: (value: unknown) => tg.Object;

		/** Assert that a value is a `tg.Object`. */
		export let assert: (value: unknown) => asserts value is tg.Object;
	}

	/** An artifact. */
	export type Artifact = tg.Directory | tg.File | tg.Symlink;

	export namespace Artifact {
		/** An artifact ID. */
		export type Id = tg.Directory.Id | tg.File.Id | tg.Symlink.Id;

		/** Get an artifact with an ID. */
		export let withId: (id: tg.Artifact.Id) => tg.Artifact;

		/** Check if a value is a `tg.Artifact`. */
		export let is: (value: unknown) => value is tg.Artifact;

		/** Expect that a value is a `tg.Artifact`. */
		export let expect: (value: unknown) => tg.Artifact;

		/** Assert that a value is a `tg.Artifact`. */
		export let assert: (value: unknown) => asserts value is tg.Artifact;
	}

	/** Create a blob. */
	export function blob(
		strings: TemplateStringsArray,
		...placeholders: tg.Args<string>
	): Promise<tg.Blob>;
	export function blob(...args: tg.Args<tg.Blob.Arg>): Promise<tg.Blob>;

	export class Blob {
		/** Get a blob with an ID. */
		static withId(id: tg.Blob.Id): tg.Blob;

		/** Create a blob. */
		static new(...args: tg.Args<tg.Blob.Arg>): Promise<tg.Blob>;

		/** Expect that a value is a `tg.Blob`. */
		static expect(value: unknown): tg.Blob;

		/** Assert that a value is a `tg.Blob`. */
		static assert(value: unknown): asserts value is tg.Blob;

		/** Get this blob's ID. */
		get id(): tg.Blob.Id;

		/** Store this blob. */
		store(): Promise<tg.Blob.Id>;

		/** Get this blob's length. */
		length(): Promise<number>;

		/** Read from this blob. */
		read(options?: tg.Blob.ReadOptions): Promise<Uint8Array>;

		/** Read this entire blob to a `Uint8Array`. */
		bytes(): Promise<Uint8Array>;

		/** Read this entire blob to a string. */
		text(): Promise<string>;
	}

	export namespace Blob {
		export type Id = string;

		export type Arg =
			| undefined
			| string
			| Uint8Array
			| tg.Blob
			| tg.Blob.Arg.Object;

		export namespace Arg {
			type Object = {
				children?: Array<tg.Blob.Child> | undefined;
			};
		}

		export type Child = { blob: tg.Blob; size: number };

		export type ReadOptions = {
			position?: number | string | undefined;
			length?: number | undefined;
		};

		export let raw: (
			strings: TemplateStringsArray,
			...placeholders: tg.Args<string>
		) => Promise<tg.Blob>;
	}

	/** Create a directory. */
	export let directory: (
		...args: Array<tg.Unresolved<tg.Directory.Arg>>
	) => Promise<tg.Directory>;

	/** A directory. */
	export class Directory {
		/** Get a directory with an ID. */
		static withId(id: tg.Directory.Id): tg.Directory;

		/** Create a directory. */
		static new(
			...args: Array<tg.Unresolved<tg.Directory.Arg>>
		): Promise<tg.Directory>;

		/** Expect that a value is a `tg.Directory`. */
		static expect(value: unknown): tg.Directory;

		/** Assert that a value is a `tg.Directory`. */
		static assert(value: unknown): asserts value is tg.Directory;

		/** Get this directory's ID. */
		get id(): tg.Directory.Id;

		/** Store this directory. */
		store(): Promise<tg.Directory.Id>;

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

		export type Arg = undefined | tg.Directory | tg.Directory.Arg.Object;

		export namespace Arg {
			type Object =
				| tg.Graph.Arg.Reference
				| {
						[key: string]:
							| undefined
							| string
							| Uint8Array
							| tg.Blob
							| tg.Artifact
							| tg.Directory.Arg.Object;
				  };
		}
	}

	/** Create a file. */
	export function file(
		strings: TemplateStringsArray,
		...placeholders: tg.Args<string>
	): Promise<tg.File>;
	export function file(...args: tg.Args<tg.File.Arg>): Promise<tg.File>;

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
		get id(): tg.File.Id;

		/** Store this file. */
		store(): Promise<tg.File.Id>;

		/** Get this file's contents. */
		contents(): Promise<tg.Blob>;

		/** Get the length of this file's contents. */
		length(): Promise<number>;

		/** Read from this file. */
		read(options?: tg.Blob.ReadOptions): Promise<Uint8Array>;

		/** Get this file's contents as a `Uint8Array`. */
		bytes(): Promise<Uint8Array>;

		/** Get this file's contents as a string. This method throws an error if the contents are not valid UTF-8. */
		text(): Promise<string>;

		/** Get this file's dependencies. */
		dependencies(): Promise<{
			[reference: tg.Reference]: tg.Referent<tg.Object> | undefined;
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
			| tg.File.Arg.Object;

		export namespace Arg {
			type Object = tg.Graph.Arg.Reference | tg.Graph.Arg.File;
		}

		export let raw: (
			strings: TemplateStringsArray,
			...placeholders: tg.Args<string>
		) => Promise<tg.File>;
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
		get id(): tg.Symlink.Id;

		/** Store this symlink. */
		store(): Promise<tg.Symlink.Id>;

		/** Get this symlink's artifact. */
		artifact(): Promise<tg.Artifact | undefined>;

		/** Get this symlink's path. */
		path(): Promise<string | undefined>;

		/** Resolve this symlink to the artifact it refers to, or return undefined if none is found. */
		resolve(): Promise<tg.Artifact | undefined>;
	}

	export namespace Symlink {
		export type Id = string;

		export type Arg =
			| string
			| tg.Artifact
			| tg.Template
			| Symlink
			| tg.Symlink.Arg.Object;

		export namespace Arg {
			type Object = tg.Graph.Arg.Reference | tg.Graph.Arg.Symlink;
		}
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
		get id(): tg.Graph.Id;

		/** Store this graph. */
		store(): Promise<tg.Graph.Id>;

		/** Get this graph's nodes. */
		nodes(): Promise<Array<tg.Graph.Node>>;
	}

	export namespace Graph {
		export type Id = string;

		export type Arg = tg.Graph | tg.Graph.Arg.Object;

		export namespace Arg {
			type Object = {
				nodes?: Array<tg.Graph.Arg.Node> | undefined;
			};

			type Node =
				| tg.Graph.Arg.DirectoryNode
				| tg.Graph.Arg.FileNode
				| tg.Graph.Arg.SymlinkNode;

			type DirectoryNode = { kind: "directory" } & tg.Graph.Arg.Directory;

			type FileNode = { kind: "file" } & tg.Graph.Arg.File;

			type SymlinkNode = { kind: "symlink" } & tg.Graph.Arg.Symlink;

			type Directory = {
				entries?:
					| { [name: string]: tg.Graph.Arg.Edge<tg.Artifact> }
					| undefined;
			};

			type File = {
				contents: tg.Blob.Arg;
				dependencies?:
					| {
							[reference: string]:
								| tg.MaybeReferent<tg.Graph.Arg.Edge<tg.Object>>
								| undefined;
					  }
					| undefined;
				executable?: boolean | undefined;
			};

			type Symlink = {
				artifact?: tg.Graph.Arg.Edge<tg.Artifact> | undefined;
				path?: string | undefined;
			};

			export type Edge<T> = tg.Graph.Arg.Reference | T;

			export type Reference = {
				graph?: tg.Graph | undefined;
				node: number;
			};
		}

		type Node =
			| tg.Graph.DirectoryNode
			| tg.Graph.FileNode
			| tg.Graph.SymlinkNode;

		type DirectoryNode = { kind: "directory" } & tg.Graph.Directory;

		type FileNode = { kind: "file" } & tg.Graph.File;

		type SymlinkNode = { kind: "symlink" } & tg.Graph.Symlink;

		type Directory = {
			entries: { [name: string]: tg.Graph.Edge<tg.Artifact> };
		};

		type File = {
			contents: tg.Blob;
			dependencies: {
				[reference: tg.Reference]: tg.Referent<tg.Graph.Edge<tg.Object>>;
			};
			executable: boolean;
		};

		type Symlink = {
			artifact: number | tg.Artifact | undefined;
			path: string | undefined;
		};

		export type Edge<T> = tg.Graph.Reference | T;

		export type Reference = {
			graph?: tg.Graph | undefined;
			index: number;
		};
	}

	/** Create a command. */
	export function command<
		A extends tg.UnresolvedArgs<Array<tg.Value>>,
		R extends tg.ReturnValue,
	>(
		function_: (...args: A) => R,
	): tg.CommandBuilder<[], tg.ResolvedReturnValue<R>>;
	export function command<
		A extends tg.UnresolvedArgs<Array<tg.Value>>,
		R extends tg.ReturnValue,
	>(
		function_: (...args: A) => R,
		...args: tg.UnresolvedArgs<tg.ResolvedArgs<A>>
	): tg.CommandBuilder<[], tg.ResolvedReturnValue<R>>;
	export function command(
		strings: TemplateStringsArray,
		...placeholders: tg.Args<tg.Template.Arg>
	): tg.CommandBuilder;
	export function command(...args: tg.Args<tg.Command.Arg>): tg.CommandBuilder;

	/** A command. */
	export class Command<
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	> {
		/** Get a command with an ID. */
		static withId(id: tg.Command.Id): tg.Command;

		/** Create a command. */
		static new<
			A extends Array<tg.Value> = Array<tg.Value>,
			R extends tg.Value = tg.Value,
		>(...args: tg.Args<tg.Command.Arg>): Promise<tg.Command<A, R>>;

		/** Expect that a value is a `tg.Command`. */
		static expect(value: unknown): tg.Command;

		/** Assert that a value is a `tg.Command`. */
		static assert(value: unknown): asserts value is tg.Command;

		/** Get this command's ID. */
		get id(): tg.Command.Id;

		/** Store this command. */
		store(): Promise<tg.Command.Id>;

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
		build(...args: tg.UnresolvedArgs<A>): tg.BuildBuilder<[], R>;

		/** Run this command and return the process's output. */
		run(...args: tg.UnresolvedArgs<A>): tg.RunBuilder<[], R>;
	}

	export namespace Command {
		export type Id = string;

		export type Arg =
			| undefined
			| string
			| tg.Artifact
			| tg.Template
			| tg.Command
			| tg.Command.Arg.Object;

		export namespace Arg {
			export type Object = {
				/** The command's arguments. */
				args?: Array<tg.Value> | undefined;

				/** The command's working directory. **/
				cwd?: string | undefined;

				/** The command's environment. */
				env?: tg.MaybeMutationMap | undefined;

				/** The command's executable. */
				executable?: tg.Command.Arg.Executable | undefined;

				/** The command's host. */
				host?: string | undefined;

				/** The command's mounts. */
				mounts?: Array<tg.Command.Mount> | undefined;

				/** The command's user. */
				user?: string | undefined;

				/** The command's stdin. */
				stdin?: tg.Blob.Arg | undefined;
			};

			export type Executable =
				| tg.Artifact
				| string
				| tg.Command.Arg.Executable.Artifact
				| tg.Command.Arg.Executable.Module
				| tg.Command.Arg.Executable.Path;

			export namespace Executable {
				export type Artifact = {
					artifact: tg.Artifact;
					path?: string | undefined;
				};

				export type Module = {
					module: tg.Module;
					export?: string | undefined;
				};

				export type Path = {
					path: string;
				};
			}
		}

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

		export type Executable =
			| tg.Command.Executable.Artifact
			| tg.Command.Executable.Module
			| tg.Command.Executable.Path;

		export namespace Executable {
			export type Artifact = {
				artifact: tg.Artifact;
				path: string | undefined;
			};

			export type Module = {
				module: tg.Module;
				export: string | undefined;
			};

			export type Path = {
				path: string;
			};
		}

		/** A mount. */
		export type Mount = {
			source: tg.Artifact;
			target: string;
		};
	}

	export namespace path {
		/** A path component. **/
		export type Component =
			| tg.path.Component.Normal
			| tg.path.Component.Current
			| tg.path.Component.Parent
			| tg.path.Component.Root;

		export namespace Component {
			export type Normal = string;

			export type Current = ".";

			export let Current: string;

			export type Parent = "..";

			export let Parent: string;

			export type Root = "/";

			export let Root: string;

			export let isNormal: (
				component: tg.path.Component,
			) => component is Normal;
		}

		/** Split a path into its components */
		export let components: (path: string) => Array<tg.path.Component>;

		/** Create a path from an array of path components. */
		export let fromComponents: (components: Array<tg.path.Component>) => string;

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
		static unset<T extends tg.Value = tg.Value>(): tg.Mutation<T>;

		/** Create a set mutation. */
		static set<T extends tg.Value = tg.Value>(
			value: tg.Unresolved<T>,
		): Promise<tg.Mutation<T>>;

		/** Create a set if unset mutation. */
		static setIfUnset<T extends tg.Value = tg.Value>(
			value: tg.Unresolved<T>,
		): Promise<tg.Mutation<T>>;

		/** Create an prepend mutation. */
		static prepend<T extends Array<tg.Value> = Array<tg.Value>>(
			values: tg.Unresolved<T>,
		): Promise<tg.Mutation<T>>;

		/** Create an append mutation. */
		static append<T extends Array<tg.Value> = Array<tg.Value>>(
			values: tg.Unresolved<T>,
		): Promise<tg.Mutation<T>>;

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
		static merge<
			T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
		>(value: tg.Unresolved<T>): Promise<tg.Mutation<T>>;

		static expect(value: unknown): tg.Mutation;

		static assert(value: unknown): asserts value is tg.Mutation;

		get inner(): tg.Mutation.Inner<T>;

		apply(map: { [key: string]: tg.Value }, key: string): Promise<void>;
	}

	export namespace Mutation {
		export type Arg<T extends tg.Value = tg.Value> =
			| { kind: "unset" }
			| { kind: "set"; value: T }
			| { kind: "set_if_unset"; value: T }
			| {
					kind: "prepend";
					values: T extends Array<infer _U> ? T : never;
			  }
			| {
					kind: "append";
					values: T extends Array<infer _U> ? T : never;
			  }
			| {
					kind: "prefix";
					template: T extends tg.Template ? T : never;
					separator?: string | undefined;
			  }
			| {
					kind: "suffix";
					template: T extends tg.Template ? T : never;
					separator?: string | undefined;
			  }
			| {
					kind: "merge";
					value: T extends { [key: string]: tg.Value } ? T : never;
			  };

		export type Inner<T extends tg.Value = tg.Value> =
			| { kind: "unset" }
			| { kind: "set"; value: T }
			| { kind: "set_if_unset"; value: T }
			| {
					kind: "prepend";
					values: T extends Array<infer _U> ? T : never;
			  }
			| {
					kind: "append";
					values: T extends Array<infer _U> ? T : never;
			  }
			| {
					kind: "prefix";
					template: T extends tg.Template ? T : never;
					separator: string | undefined;
			  }
			| {
					kind: "suffix";
					template: T extends tg.Template ? T : never;
					separator: string | undefined;
			  }
			| {
					kind: "merge";
					value: T extends { [key: string]: tg.Value } ? T : never;
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
		export type Arg = undefined | tg.Template.Component | tg.Template;

		export type Component = string | tg.Artifact;

		/** A tagged template function that behaves identically to `tg.template` except that it does not trim leading whitespace. **/
		export let raw: (
			strings: TemplateStringsArray,
			...placeholders: tg.Args<tg.Template.Arg>
		) => Promise<tg.Template>;
	}

	/* Compute a checksum. */
	export let checksum: (
		input: string | Uint8Array | tg.Blob | tg.Artifact,
		algorithm: tg.Checksum.Algorithm,
	) => Promise<tg.Checksum>;

	/** A checksum. */
	export type Checksum = `${tg.Checksum.Algorithm}${":" | "-"}${string}`;

	export namespace Checksum {
		export type Algorithm = "blake3" | "sha256" | "sha512";

		/** Check if a value is a `tg.Checksum`. */
		export let is: (value: unknown) => value is tg.Checksum;

		/** Expect that a value is a `tg.Checksum`. */
		export let expect: (value: unknown) => tg.Checksum;

		/** Assert that a value is a `tg.Checksum`. */
		export let assert: (value: unknown) => asserts value is tg.Checksum;

		export let new_: (
			input: string | Uint8Array | tg.Blob | tg.Artifact,
			algorithm: tg.Checksum.Algorithm,
		) => tg.Checksum;
		export type { new_ as new };
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

	export type Module = {
		kind: tg.Module.Kind;
		referent: tg.Referent<tg.Object>;
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

	export function build<
		A extends tg.UnresolvedArgs<Array<tg.Value>>,
		R extends tg.ReturnValue,
	>(
		function_: (...args: A) => R,
	): tg.BuildBuilder<[], tg.ResolvedReturnValue<R>>;
	export function build<
		A extends tg.UnresolvedArgs<Array<tg.Value>>,
		R extends tg.ReturnValue,
	>(
		function_: (...args: A) => R,
		...args: tg.UnresolvedArgs<tg.ResolvedArgs<A>>
	): tg.BuildBuilder<[], tg.ResolvedReturnValue<R>>;
	export function build(
		strings: TemplateStringsArray,
		...placeholders: tg.Args<tg.Template.Arg>
	): tg.BuildBuilder;
	export function build(...args: tg.Args<tg.Process.BuildArg>): tg.BuildBuilder;

	export function run<
		A extends tg.UnresolvedArgs<Array<tg.Value>>,
		R extends tg.ReturnValue,
	>(function_: (...args: A) => R): tg.RunBuilder<[], tg.ResolvedReturnValue<R>>;
	export function run<
		A extends tg.UnresolvedArgs<Array<tg.Value>>,
		R extends tg.ReturnValue,
	>(
		function_: (...args: A) => R,
		...args: tg.UnresolvedArgs<tg.ResolvedArgs<A>>
	): tg.RunBuilder<[], tg.ResolvedReturnValue<R>>;
	export function run(
		strings: TemplateStringsArray,
		...placeholders: tg.Args<tg.Template.Arg>
	): tg.RunBuilder;
	export function run(...args: tg.Args<tg.Process.RunArg>): tg.RunBuilder;

	export let process: {
		args: Array<tg.Value>;
		cwd: string;
		env: { [key: string]: tg.Value };
		executable: tg.Command.Executable;
	};

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
		get id(): tg.Process.Id;

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

		type BuildArgObject = {
			/** The command's arguments. */
			args?: Array<tg.Value> | undefined;

			/** If a checksum of the process's output is provided, then the process can be cached even if it is not sandboxed. */
			checksum?: tg.Checksum | undefined;

			/** The command's working directory. **/
			cwd?: string | undefined;

			/** The command's environment. */
			env?: tg.MaybeMutationMap | undefined;

			/** The command's executable. */
			executable?: tg.Command.Arg.Executable | undefined;

			/** The command's host. */
			host?: string | undefined;

			/** The command's mounts. */
			mounts?: Array<tg.Command.Mount> | undefined;

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

		type RunArgObject = {
			/** The command's arguments. */
			args?: Array<tg.Value> | undefined;

			/** If a checksum of the process's output is provided, then the process can be cached even if it is not sandboxed. */
			checksum?: tg.Checksum | undefined;

			/** The command's working directory. **/
			cwd?: string | undefined;

			/** The command's environment. */
			env?: tg.MaybeMutationMap | undefined;

			/** The command's executable. */
			executable?: tg.Command.Arg.Executable | undefined;

			/** The command's host. */
			host?: string | undefined;

			/** The command's or process's mounts. */
			mounts?: Array<tg.Command.Mount | tg.Process.Mount> | undefined;

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
	}

	export interface BuildBuilder<
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	> {
		// biome-ignore lint/style/useShorthandFunctionType: This is necessary to make this callable.
		(...args: tg.UnresolvedArgs<A>): tg.BuildBuilder<[], R>;
	}

	export class BuildBuilder<
		// biome-ignore lint/correctness/noUnusedVariables: <reason>
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	> extends Function {
		constructor(...args: tg.Args<tg.Process.BuildArgObject>);

		arg(...args: Array<tg.Unresolved<tg.Value>>): this;

		args(
			...args: Array<tg.Unresolved<tg.MaybeMutation<Array<tg.Value>>>>
		): this;

		cwd(cwd: tg.Unresolved<tg.MaybeMutation<string | undefined>>): this;

		env(
			...envs: Array<tg.Unresolved<tg.MaybeMutation<tg.MaybeMutationMap>>>
		): this;

		executable(
			executable: tg.Unresolved<tg.MaybeMutation<tg.Command.Arg.Executable>>,
		): this;

		host(host: tg.Unresolved<tg.MaybeMutation<string>>): this;

		mount(...mounts: Array<tg.Unresolved<tg.Command.Mount>>): this;

		mounts(
			...mounts: Array<tg.Unresolved<tg.MaybeMutation<Array<tg.Command.Mount>>>>
		): this;

		named(name: tg.Unresolved<tg.MaybeMutation<string | undefined>>): this;

		network(network: tg.Unresolved<tg.MaybeMutation<boolean>>): this;

		// biome-ignore lint/suspicious/noThenProperty: This is necessary to make this thenable.
		then<TResult1 = R, TResult2 = never>(
			this: tg.BuildBuilder<[], R>,
			onfulfilled?:
				| ((value: R) => TResult1 | PromiseLike<TResult1>)
				| undefined
				| null,
			onrejected?:
				| ((reason: any) => TResult2 | PromiseLike<TResult2>)
				| undefined
				| null,
		): PromiseLike<TResult1 | TResult2>;
	}

	export interface CommandBuilder<
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	> {
		// biome-ignore lint/style/useShorthandFunctionType: This is necessary to make this callable.
		(...args: tg.UnresolvedArgs<A>): tg.CommandBuilder<[], R>;
	}

	export class CommandBuilder<
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	> extends Function {
		constructor(...args: tg.Args<tg.Command.Arg.Object>);

		arg(...args: Array<tg.Unresolved<tg.Value>>): this;

		args(
			...args: Array<tg.Unresolved<tg.MaybeMutation<Array<tg.Value>>>>
		): this;

		cwd(cwd: tg.Unresolved<tg.MaybeMutation<string | undefined>>): this;

		env(
			...envs: Array<tg.Unresolved<tg.MaybeMutation<tg.MaybeMutationMap>>>
		): this;

		executable(
			executable: tg.Unresolved<tg.MaybeMutation<tg.Command.Arg.Executable>>,
		): this;

		host(host: tg.Unresolved<tg.MaybeMutation<string>>): this;

		mount(...mounts: Array<tg.Unresolved<tg.Command.Mount>>): this;

		mounts(
			...mounts: Array<tg.Unresolved<tg.MaybeMutation<Array<tg.Command.Mount>>>>
		): this;

		/** Build this command and return the process's output. */
		build(...args: tg.UnresolvedArgs<A>): tg.BuildBuilder<[], R>;

		/** Run this command and return the process's output. */
		run(...args: tg.UnresolvedArgs<A>): tg.RunBuilder<[], R>;

		// biome-ignore lint/suspicious/noThenProperty: This is necessary to make this thenable.
		then<TResult1 = tg.Command<A, R>, TResult2 = never>(
			onfulfilled?:
				| ((value: tg.Command<A, R>) => TResult1 | PromiseLike<TResult1>)
				| undefined
				| null,
			onrejected?:
				| ((reason: any) => TResult2 | PromiseLike<TResult2>)
				| undefined
				| null,
		): PromiseLike<TResult1 | TResult2>;
	}

	export interface RunBuilder<
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	> {
		// biome-ignore lint/style/useShorthandFunctionType: This is necessary to make this callable.
		(...args: tg.UnresolvedArgs<A>): tg.RunBuilder<[], R>;
	}

	export class RunBuilder<
		// biome-ignore lint/correctness/noUnusedVariables: <reason>
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	> extends Function {
		constructor(...args: tg.Args<tg.Process.RunArgObject>);

		arg(...args: Array<tg.Unresolved<tg.Value>>): this;

		args(
			...args: Array<tg.Unresolved<tg.MaybeMutation<Array<tg.Value>>>>
		): this;

		cwd(cwd: tg.Unresolved<tg.MaybeMutation<string | undefined>>): this;

		env(
			...envs: Array<tg.Unresolved<tg.MaybeMutation<tg.MaybeMutationMap>>>
		): this;

		executable(
			executable: tg.Unresolved<tg.MaybeMutation<tg.Command.Arg.Executable>>,
		): this;

		host(host: tg.Unresolved<tg.MaybeMutation<string>>): this;

		mount(
			...mounts: Array<tg.Unresolved<tg.Command.Mount | tg.Process.Mount>>
		): this;

		mounts(
			...mounts: Array<
				tg.Unresolved<
					tg.MaybeMutation<Array<tg.Command.Mount | tg.Process.Mount>>
				>
			>
		): this;

		named(name: tg.Unresolved<tg.MaybeMutation<string | undefined>>): this;

		network(network: tg.Unresolved<tg.MaybeMutation<boolean>>): this;

		// biome-ignore lint/suspicious/noThenProperty: This is necessary to make this thenable.
		then<TResult1 = R, TResult2 = never>(
			this: tg.RunBuilder<[], R>,
			onfulfilled?:
				| ((value: R) => TResult1 | PromiseLike<TResult1>)
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
		options: tg.Referent.Options;
	};

	export namespace Referent {
		export type Options = {
			artifact?: tg.Artifact.Id | undefined;
			id?: tg.Object.Id | undefined;
			path?: string | undefined;
			tag?: tg.Tag | undefined;
		};
	}

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
	 * This computed type performs the inverse of `tg.Unresolved`. It takes a type and returns the output of calling `tg.resolve` on a value of that type. Here are some examples:
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

	type Args<T extends tg.Value = tg.Value> = Array<
		tg.Unresolved<tg.ValueOrMaybeMutationMap<T>>
	>;

	type MaybePromise<T> = T | Promise<T>;

	type MaybeMutation<T extends tg.Value = tg.Value> = T | tg.Mutation<T>;

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

	type ValueOrMaybeMutationMap<T extends tg.Value = tg.Value> = T extends
		| undefined
		| boolean
		| number
		| string
		| tg.Object
		| Uint8Array
		| tg.Mutation
		| tg.Template
		| Array<infer _U extends tg.Value>
		? T
		: T extends { [key: string]: tg.Value }
			? tg.MaybeMutationMap<T>
			: never;

	type MaybeReferent<T> = T | tg.Referent<T>;

	type UnresolvedArgs<T extends Array<tg.Value>> = {
		[K in keyof T]: tg.Unresolved<T[K]>;
	};

	type ResolvedArgs<T extends Array<tg.Unresolved<tg.Value>>> = {
		[K in keyof T]: tg.Resolved<T[K]>;
	};

	type ReturnValue = tg.MaybePromise<void> | tg.Unresolved<tg.Value>;

	type ResolvedReturnValue<T extends tg.ReturnValue> =
		T extends tg.MaybePromise<void>
			? undefined
			: T extends tg.Unresolved<tg.Value>
				? tg.Resolved<T>
				: never;
}
