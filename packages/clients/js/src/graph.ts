import * as tg from "./index.ts";

export let graph = async (
	...args: tg.Args<tg.Graph.Arg>
): Promise<tg.Graph> => {
	return await tg.Graph.new(...args);
};

export class Graph {
	#state: tg.Object.State;

	constructor(arg: {
		id?: tg.Graph.Id;
		object?: tg.Graph.Object;
		stored: boolean;
	}) {
		let object =
			arg.object !== undefined
				? { kind: "graph" as const, value: arg.object }
				: undefined;
		this.#state = new tg.Object.State({
			id: arg.id,
			object,
			stored: arg.stored,
		});
	}

	get state(): tg.Object.State {
		return this.#state;
	}

	static withId(id: tg.Graph.Id): tg.Graph {
		return new tg.Graph({ id, stored: true });
	}

	static withObject(object: tg.Graph.Object): tg.Graph {
		return new tg.Graph({ object, stored: false });
	}

	static fromData(data: tg.Graph.Data): tg.Graph {
		return tg.Graph.withObject(tg.Graph.Object.fromData(data));
	}

	static async new(...args: tg.Args<tg.Graph.Arg>): Promise<tg.Graph> {
		let arg = await tg.Graph.arg(...args);
		let nodes = await Promise.all(
			(arg.nodes ?? []).map(async (node) => {
				if (node.kind === "directory") {
					let entries = Object.fromEntries(
						Object.entries(node.entries ?? {}).map(([name, edge]) => {
							return [name, tg.Graph.Edge.fromArg(edge, arg.nodes)];
						}),
					);
					return {
						kind: "directory" as const,
						entries,
					};
				} else if (node.kind === "file") {
					let contents = await tg.blob(node.contents);
					let dependencies = Object.fromEntries(
						Object.entries(node.dependencies ?? {}).map(
							([reference, value]) => {
								if (!value) {
									return [reference, undefined];
								}
								let dependency: tg.Graph.Dependency;
								if (
									typeof value === "number" ||
									"index" in value ||
									tg.Object.is(value)
								) {
									let item = tg.Graph.Edge.fromArg(value, arg.nodes);
									dependency = { item, options: {} };
								} else if (value.item === undefined) {
									dependency = { item: undefined, options: value.options };
								} else {
									let item = tg.Graph.Edge.fromArg(value.item, arg.nodes);
									dependency = { item, options: value.options };
								}
								return [reference, dependency];
							},
						),
					);
					let executable = node.executable ?? false;
					let module = node.module ?? undefined;
					return {
						kind: "file" as const,
						contents,
						dependencies,
						executable,
						module,
					};
				} else if (node.kind === "symlink") {
					let artifact = tg.Graph.Edge.fromArg(node.artifact, arg.nodes);
					let path = node.path;
					return {
						kind: "symlink" as const,
						artifact,
						path,
					};
				} else {
					return tg.unreachable();
				}
			}),
		);
		return tg.Graph.withObject({ nodes });
	}

	static async arg(
		...args: tg.Args<tg.Graph.Arg>
	): Promise<tg.Graph.Arg.Object> {
		let resolved = await Promise.all(args.map(tg.resolve));
		let nodes = [];
		let offset = 0;
		for (let arg of resolved) {
			let argNodes =
				arg instanceof tg.Graph
					? await arg.nodes()
					: Array.isArray(arg.nodes)
						? arg.nodes
						: [];
			for (let argNode of argNodes) {
				if (argNode.kind === "directory") {
					let node: tg.Graph.Arg.DirectoryNode = { kind: "directory" };
					if ("entries" in argNode) {
						if (argNode.entries !== undefined) {
							node.entries = {};
							for (let name in argNode.entries) {
								let entry = argNode.entries[name];
								if (typeof entry === "number") {
									node.entries[name] = entry + offset;
								} else if (
									typeof entry === "object" &&
									"index" in entry &&
									entry.graph === undefined
								) {
									entry.index += offset;
									node.entries[name] = entry;
								} else if (entry) {
									node.entries[name] = entry;
								}
							}
						} else {
							node.entries = argNode.entries;
						}
					}
					nodes.push(node);
				} else if (argNode.kind === "file") {
					let node: tg.Graph.Arg.FileNode = {
						kind: "file" as const,
					};
					if ("contents" in argNode) {
						node.contents = argNode.contents;
					}
					if ("dependencies" in argNode) {
						if (argNode.dependencies !== undefined) {
							node.dependencies = {};
							for (let reference in argNode.dependencies) {
								let value = argNode.dependencies[reference]!;
								let dependency: tg.Referent<
									tg.Graph.Arg.Edge<tg.Object> | undefined
								>;
								if (
									typeof value === "number" ||
									"index" in value ||
									tg.Object.is(value)
								) {
									dependency = { item: value, options: {} };
								} else {
									dependency = value;
								}
								if (dependency.item === undefined) {
									node.dependencies[reference] = dependency;
								} else if (typeof dependency.item === "number") {
									node.dependencies[reference] = {
										item: dependency.item + offset,
										options: dependency.options,
									};
								} else if ("index" in dependency.item) {
									node.dependencies[reference] = {
										item: {
											...dependency.item,
											index: dependency.item.index + offset,
										},
										options: dependency.options,
									};
								} else if (tg.Object.is(dependency.item)) {
									node.dependencies[reference] = dependency;
								}
							}
						} else {
							node.dependencies = argNode.dependencies;
						}
					}
					if ("executable" in argNode) {
						node.executable = argNode.executable;
					}
					nodes.push(node);
				} else if (argNode.kind === "symlink") {
					let artifact: tg.Graph.Arg.Edge<tg.Artifact> | undefined;
					if (typeof argNode.artifact === "number") {
						artifact = argNode.artifact + offset;
					} else if (
						argNode.artifact !== undefined &&
						"index" in argNode.artifact
					) {
						artifact = {
							...argNode.artifact,
							index: argNode.artifact.index + offset,
						};
					} else {
						artifact = argNode.artifact;
					}
					nodes.push({
						kind: "symlink" as const,
						artifact: artifact,
						path: argNode.path,
					});
				} else {
					return tg.unreachable();
				}
			}
			offset += argNodes.length;
		}
		return { nodes };
	}

	static expect(value: unknown): tg.Graph {
		tg.assert(value instanceof tg.Graph);
		return value;
	}

	static assert(value: unknown): asserts value is tg.Graph {
		tg.assert(value instanceof tg.Graph);
	}

	get id(): tg.Graph.Id {
		let id = this.#state.id;
		tg.assert(tg.Object.Id.kind(id) === "graph");
		return id;
	}

	async object(): Promise<tg.Graph.Object> {
		let object = await this.#state.load();
		tg.assert(object.kind === "graph");
		return object.value;
	}

	async load(): Promise<tg.Graph.Object> {
		let object = await this.#state.load();
		tg.assert(object.kind === "graph");
		return object.value;
	}

	unload(): void {
		this.#state.unload();
	}

	async store(): Promise<tg.Graph.Id> {
		await tg.Value.store(this);
		return this.id;
	}

	async children(): Promise<Array<tg.Object>> {
		return this.#state.children();
	}

	async nodes(): Promise<Array<tg.Graph.Node>> {
		return (await this.object()).nodes;
	}

	async get(index: number): Promise<tg.Artifact> {
		let nodes = await this.nodes();
		let node = nodes[index];
		tg.assert(node !== undefined, "invalid graph index");
		switch (node.kind) {
			case "directory": {
				return tg.Directory.withObject({
					graph: this,
					index,
					kind: "directory",
				});
			}
			case "file": {
				return tg.File.withObject({ graph: this, index, kind: "file" });
			}
			case "symlink": {
				return tg.Symlink.withObject({
					graph: this,
					index,
					kind: "symlink",
				});
			}
		}
	}
}

export namespace Graph {
	export type Id = string;

	export type Arg = tg.Graph | tg.Graph.Arg.Object;

	export namespace Arg {
		export type Object = {
			nodes?: Array<tg.Graph.Arg.Node> | undefined;
		};

		export type Node =
			| tg.Graph.Arg.DirectoryNode
			| tg.Graph.Arg.FileNode
			| tg.Graph.Arg.SymlinkNode;
		export type DirectoryNode = { kind: "directory" } & tg.Graph.Arg.Directory;
		export type FileNode = { kind: "file" } & tg.Graph.Arg.File;
		export type SymlinkNode = { kind: "symlink" } & tg.Graph.Arg.Symlink;

		export type Directory = {
			entries?: { [name: string]: tg.Graph.Arg.Edge<tg.Artifact> } | undefined;
		};

		export type Dependency = tg.MaybeReferent<
			tg.Graph.Arg.Edge<tg.Object> | undefined
		>;

		export type File = {
			contents?: tg.Blob.Arg | undefined;
			dependencies?:
				| {
						[reference: tg.Reference]: tg.Graph.Arg.Dependency | undefined;
				  }
				| undefined;
			executable?: boolean | undefined;
			module?: string | undefined;
		};

		export type Symlink = {
			artifact?: tg.Graph.Arg.Edge<tg.Artifact> | undefined;
			path?: string | undefined;
		};

		export type Edge<T> = tg.Graph.Arg.Pointer | T;

		export type Pointer =
			| number
			| {
					graph?: tg.Graph | undefined;
					index: number;
					kind?: tg.Artifact.Kind;
			  };

		export namespace Pointer {
			export let is = (value: unknown): value is tg.Graph.Arg.Pointer => {
				return (
					typeof value === "number" ||
					(typeof value === "object" &&
						value !== null &&
						"index" in value &&
						typeof value.index === "number")
				);
			};
		}
	}

	export type Object = {
		nodes: Array<Node>;
	};

	export namespace Object {
		export let toData = (object: tg.Graph.Object): tg.Graph.Data => {
			return {
				nodes: object.nodes.map(tg.Graph.Node.toData),
			};
		};

		export let fromData = (data: tg.Graph.Data): tg.Graph.Object => {
			return {
				nodes: data.nodes.map(tg.Graph.Node.fromData),
			};
		};

		export let children = (object: tg.Graph.Object): Array<tg.Object> => {
			return object.nodes.flatMap(tg.Graph.Node.children);
		};
	}

	export type Node =
		| tg.Graph.DirectoryNode
		| tg.Graph.FileNode
		| tg.Graph.SymlinkNode;
	export type DirectoryNode = { kind: "directory" } & tg.Graph.Directory;
	export type FileNode = { kind: "file" } & tg.Graph.File;
	export type SymlinkNode = { kind: "symlink" } & tg.Graph.Symlink;

	export namespace Node {
		export let toData = (object: tg.Graph.Node): tg.Graph.Data.Node => {
			if (object.kind === "directory") {
				return {
					kind: "directory",
					...tg.Graph.Directory.toData(object),
				};
			} else if (object.kind === "file") {
				return {
					kind: "file",
					...tg.Graph.File.toData(object),
				};
			} else if (object.kind === "symlink") {
				return {
					kind: "symlink",
					...tg.Graph.Symlink.toData(object),
				};
			} else {
				return tg.unreachable();
			}
		};

		export let fromData = (data: tg.Graph.Data.Node): tg.Graph.Node => {
			if (data.kind === "directory") {
				return {
					kind: "directory",
					...tg.Graph.Directory.fromData(data),
				};
			} else if (data.kind === "file") {
				return {
					kind: "file",
					...tg.Graph.File.fromData(data),
				};
			} else if (data.kind === "symlink") {
				return {
					kind: "symlink",
					...tg.Graph.Symlink.fromData(data),
				};
			} else {
				return tg.unreachable();
			}
		};

		export let children = (node: Node): Array<tg.Object> => {
			switch (node.kind) {
				case "directory": {
					return tg.Graph.Directory.children(node);
				}
				case "file": {
					return tg.Graph.File.children(node);
				}
				case "symlink": {
					return tg.Graph.Symlink.children(node);
				}
			}
		};
	}

	export type Directory = {
		entries: { [name: string]: tg.Graph.Edge<tg.Artifact> };
	};

	export namespace Directory {
		export let toData = (
			object: tg.Graph.Directory,
		): tg.Graph.Data.Directory => {
			let data: tg.Graph.Data.Directory = {};
			if (globalThis.Object.entries(object.entries).length > 0) {
				data.entries = globalThis.Object.fromEntries(
					globalThis.Object.entries(object.entries).map(([name, artifact]) => [
						name,
						tg.Graph.Edge.toData(artifact, (artifact) => artifact.id),
					]),
				);
			}
			return data;
		};

		export let fromData = (
			data: tg.Graph.Data.Directory,
		): tg.Graph.Directory => {
			return {
				entries: globalThis.Object.fromEntries(
					globalThis.Object.entries(data.entries ?? {}).map(([name, edge]) => [
						name,
						tg.Graph.Edge.fromData(edge, tg.Artifact.withId),
					]),
				),
			};
		};

		export let children = (object: tg.Graph.Directory): Array<tg.Object> => {
			return globalThis.Object.entries(object.entries).flatMap(([_, edge]) =>
				tg.Graph.Edge.children(edge),
			);
		};
	}

	export type File = {
		contents: tg.Blob;
		dependencies: {
			[reference: tg.Reference]: tg.Graph.Dependency | undefined;
		};
		executable: boolean;
		module: string | undefined;
	};

	export namespace File {
		export let toData = (object: tg.Graph.File): tg.Graph.Data.File => {
			let data: tg.Graph.Data.File = {};
			data.contents = object.contents.id;
			if (globalThis.Object.entries(object.dependencies).length > 0) {
				data.dependencies = globalThis.Object.fromEntries(
					globalThis.Object.entries(object.dependencies).map(
						([reference, dependency]) => {
							if (!dependency) {
								return [reference, undefined];
							}
							return [reference, tg.Graph.Dependency.toDataString(dependency)];
						},
					),
				);
			}
			if (object.executable !== false) {
				data.executable = object.executable;
			}
			return data;
		};

		export let fromData = (data: tg.Graph.Data.File): tg.Graph.File => {
			tg.assert(data.contents !== undefined);
			return {
				contents: tg.Blob.withId(data.contents),
				dependencies: globalThis.Object.fromEntries(
					globalThis.Object.entries(data.dependencies ?? {}).map(
						([reference, dependency]) => {
							if (!dependency) {
								return [reference, undefined];
							}
							return [
								reference,
								typeof dependency === "string"
									? tg.Graph.Dependency.fromDataString(dependency)
									: tg.Referent.fromData(dependency, (item) =>
											item !== undefined
												? tg.Graph.Edge.fromData(item, tg.Object.withId)
												: undefined,
										),
							];
						},
					),
				),
				executable: data.executable ?? false,
				module: data.module ?? undefined,
			};
		};

		export let children = (object: tg.Graph.File): Array<tg.Object> => {
			let dependencies = globalThis.Object.entries(object.dependencies)
				.filter(
					([_, dependency]) =>
						dependency !== undefined && dependency.item !== undefined,
				)
				.flatMap(([_, dependency]) =>
					tg.Graph.Edge.children(dependency!.item!),
				);
			return [object.contents, ...dependencies];
		};
	}

	export type Dependency = tg.Referent<tg.Graph.Edge<tg.Object> | undefined>;

	export namespace Dependency {
		export let toDataString = (value: tg.Graph.Dependency): string => {
			let item =
				value.item !== undefined
					? tg.Graph.Edge.toDataString(value.item, (item) => item.id)
					: "";
			let params = [];
			if (value.options?.artifact !== undefined) {
				params.push(`artifact=${encodeURIComponent(value.options.artifact)}`);
			}
			if (value.options?.id !== undefined) {
				params.push(`id=${encodeURIComponent(value.options.id)}`);
			}
			if (value.options?.name !== undefined) {
				params.push(`name=${encodeURIComponent(value.options.name)}`);
			}
			if (value.options?.path !== undefined) {
				params.push(`path=${encodeURIComponent(value.options.path)}`);
			}
			if (value.options?.tag !== undefined) {
				params.push(`tag=${encodeURIComponent(value.options.tag)}`);
			}
			if (params.length > 0) {
				item += "?";
				item += params.join("&");
			}
			return item;
		};

		export let fromDataString = (data: string): tg.Graph.Dependency => {
			let [itemString, params] = data.split("?");
			let item: tg.Graph.Edge<tg.Object> | undefined;
			if (itemString !== undefined && itemString !== "") {
				item = tg.Graph.Edge.fromDataString(itemString, tg.Object.withId);
			}
			let options: tg.Referent.Options = {};
			if (params !== undefined) {
				for (let param of params.split("&")) {
					let [key, value] = param.split("=");
					if (value === undefined) {
						throw new Error("missing value");
					}
					switch (key) {
						case "artifact": {
							options.artifact = decodeURIComponent(value);
							break;
						}
						case "id": {
							options.id = decodeURIComponent(value);
							break;
						}
						case "name": {
							options.name = decodeURIComponent(value);
							break;
						}
						case "path": {
							options.path = decodeURIComponent(value);
							break;
						}
						case "tag": {
							options.tag = decodeURIComponent(value);
							break;
						}
						default: {
							throw new Error("invalid key");
						}
					}
				}
			}
			return { item, options };
		};

		export type Data =
			| string
			| {
					item: tg.Graph.Data.Edge<tg.Object.Id> | undefined;
					options?: tg.Referent.Data.Options;
			  };
	}

	export type Symlink = {
		artifact: tg.Graph.Edge<tg.Artifact> | undefined;
		path: string | undefined;
	};

	export namespace Symlink {
		export let toData = (object: tg.Graph.Symlink): tg.Graph.Data.Symlink => {
			let data: tg.Graph.Data.Symlink = {};
			if (object.artifact !== undefined) {
				data.artifact = tg.Graph.Edge.toData(
					object.artifact,
					(artifact) => artifact.id,
				);
			}
			if (object.path !== undefined) {
				data.path = object.path;
			}
			return data;
		};

		export let fromData = (data: tg.Graph.Data.Symlink): tg.Graph.Symlink => {
			return {
				artifact:
					data.artifact !== undefined
						? tg.Graph.Edge.fromData(data.artifact, tg.Artifact.withId)
						: undefined,
				path: data.path,
			};
		};

		export let children = (object: tg.Graph.Symlink): Array<tg.Object> => {
			if (object.artifact !== undefined) {
				return tg.Graph.Edge.children(object.artifact);
			} else {
				return [];
			}
		};
	}

	export type Edge<T> = tg.Graph.Pointer | T;

	export namespace Edge {
		export let fromArg = <T>(
			arg: tg.Graph.Arg.Edge<T>,
			nodes?: Array<tg.Graph.Arg.Node>,
		): tg.Graph.Edge<T> => {
			if (typeof arg === "number") {
				if (nodes === undefined) {
					throw new Error(
						"cannot convert number to edge without nodes context",
					);
				}
				let kind = nodes[arg]?.kind;
				if (!kind) {
					throw new Error(`invalid node index: ${arg}`);
				}
				return { index: arg, kind };
			} else if (
				typeof arg === "object" &&
				arg !== null &&
				"index" in arg &&
				typeof arg.index === "number"
			) {
				let reference = arg as {
					graph?: tg.Graph;
					index: number;
					kind?: tg.Artifact.Kind;
				};
				if (reference.kind !== undefined) {
					return reference as tg.Graph.Pointer;
				}
				if (nodes === undefined) {
					throw new Error("cannot infer kind without nodes context");
				}
				let kind = nodes[reference.index]?.kind;
				if (!kind) {
					throw new Error(`invalid node index: ${reference.index}`);
				}
				return { ...reference, kind };
			} else {
				return arg as T;
			}
		};

		export let toData = <T, U>(
			object: tg.Graph.Edge<T>,
			f: (item: T) => U,
		): tg.Graph.Data.Edge<U> => {
			if (typeof object === "object" && object !== null && "index" in object) {
				return tg.Graph.Pointer.toData(object);
			} else {
				return f(object);
			}
		};

		export let fromData = <T, U>(
			data: tg.Graph.Data.Edge<T>,
			f: (item: T) => U,
		): tg.Graph.Edge<U> => {
			if (
				typeof data === "string" ||
				(typeof data === "object" && data !== null && "index" in data)
			) {
				try {
					return tg.Graph.Pointer.fromData(data);
				} catch {}
			}
			return f(data as T);
		};

		export let toDataString = <T, U extends string>(
			object: tg.Graph.Edge<T>,
			f: (item: T) => U,
		): string => {
			if (typeof object === "object" && object !== null && "index" in object) {
				return tg.Graph.Pointer.toDataString(object);
			} else {
				return f(object);
			}
		};

		export let fromDataString = <T>(
			data: string,
			f: (item: string) => T,
		): tg.Graph.Edge<T> => {
			if (data.includes("index=")) {
				return tg.Graph.Pointer.fromDataString(data);
			} else {
				return f(data);
			}
		};

		export let children = <T>(object: tg.Graph.Edge<T>): Array<tg.Object> => {
			if (typeof object === "number") {
				return [];
			} else if (
				typeof object === "object" &&
				object !== null &&
				"index" in object
			) {
				return tg.Graph.Pointer.children(object);
			} else if (tg.Object.is(object)) {
				return [object];
			} else {
				return [];
			}
		};
	}

	export type Pointer = {
		graph?: tg.Graph | undefined;
		index: number;
		kind: tg.Artifact.Kind;
	};

	export namespace Pointer {
		export let fromArg = (arg: tg.Graph.Arg.Pointer): tg.Graph.Pointer => {
			if (typeof arg === "number" || arg.kind === undefined) {
				throw new Error("expected the kind field to be set");
			}
			return arg as tg.Graph.Pointer;
		};

		export let toData = (object: tg.Graph.Pointer): tg.Graph.Data.Pointer => {
			let data: { graph?: tg.Graph.Id; index: number; kind: tg.Artifact.Kind };
			if (object.graph !== undefined) {
				data = {
					graph: object.graph.id,
					index: object.index,
					kind: object.kind,
				};
			} else {
				data = { index: object.index, kind: object.kind };
			}
			return data;
		};

		export let fromData = (data: tg.Graph.Data.Pointer): tg.Graph.Pointer => {
			if (typeof data === "string") {
				return tg.Graph.Pointer.fromDataString(data);
			} else {
				let graph = data.graph ? tg.Graph.withId(data.graph) : undefined;
				return { graph, index: data.index, kind: data.kind };
			}
		};

		export let toDataString = (object: tg.Graph.Pointer): string => {
			let string = "";
			if (object.graph !== undefined) {
				string += `graph=${object.graph.id}&`;
			}
			string += `index=${object.index}`;
			string += `&kind=${object.kind}`;
			return string;
		};

		export let fromDataString = (data: string): tg.Graph.Pointer => {
			let graph: tg.Graph | undefined;
			let index: number | undefined;
			let kind: tg.Artifact.Kind | undefined;
			for (let param of data.split("&")) {
				let [key, value] = param.split("=");
				if (value === undefined) {
					throw new Error("missing value");
				}
				switch (key) {
					case "graph": {
						graph = tg.Graph.withId(decodeURIComponent(value));
						break;
					}
					case "index": {
						index = Number.parseInt(decodeURIComponent(value), 10);
						break;
					}
					case "kind": {
						kind = decodeURIComponent(value) as tg.Artifact.Kind;
						break;
					}
					default: {
						throw new Error("invalid key");
					}
				}
			}
			tg.assert(index !== undefined, "missing index");
			tg.assert(kind !== undefined, "missing kind");
			return { graph, index, kind };
		};

		export let children = (object: tg.Graph.Pointer): Array<tg.Object> => {
			if (object.graph !== undefined) {
				return [object.graph];
			} else {
				return [];
			}
		};

		export let is = (value: unknown): value is tg.Graph.Pointer => {
			return (
				typeof value === "object" &&
				value !== null &&
				"index" in value &&
				typeof value.index === "number"
			);
		};
	}

	export type Data = {
		nodes: Array<tg.Graph.Data.Node>;
	};

	export namespace Data {
		export type Node =
			| tg.Graph.Data.DirectoryNode
			| tg.Graph.Data.FileNode
			| tg.Graph.Data.SymlinkNode;
		export type DirectoryNode = { kind: "directory" } & tg.Graph.Data.Directory;
		export type FileNode = { kind: "file" } & tg.Graph.Data.File;
		export type SymlinkNode = { kind: "symlink" } & tg.Graph.Data.Symlink;

		export type Directory = {
			entries?: { [name: string]: tg.Graph.Data.Edge<tg.Artifact.Id> };
		};

		export type File = {
			contents?: tg.Blob.Id;
			dependencies?: {
				[reference: tg.Reference]: tg.Graph.Dependency.Data | undefined;
			};
			executable?: boolean;
			module?: string;
		};

		export type Symlink = {
			artifact?: tg.Graph.Data.Edge<tg.Artifact.Id>;
			path?: string;
		};

		export type Edge<T> = tg.Graph.Data.Pointer | T;

		export type Pointer =
			| string
			| {
					graph?: tg.Graph.Id;
					index: number;
					kind: tg.Artifact.Kind;
			  };

		export namespace Pointer {
			export let is = (value: unknown): value is tg.Graph.Data.Pointer => {
				return (
					typeof value === "number" ||
					typeof value === "string" ||
					(typeof value === "object" &&
						value !== null &&
						"index" in value &&
						typeof value.index === "number")
				);
			};
		}
	}
}
