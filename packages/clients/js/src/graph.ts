import * as tg from "./index.ts";

export let graph = async (
	...args: tg.Args<tg.Graph.Arg>
): Promise<tg.Graph> => {
	return await tg.Graph.new(...args);
};

export class Graph {
	#state: tg.Graph.State;

	constructor(state: tg.Graph.State) {
		this.#state = state;
	}

	get state(): tg.Graph.State {
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
							return [name, tg.Graph.Edge.fromArg(edge)];
						}),
					);
					return {
						kind: "directory" as const,
						entries,
					};
				} else if (node.kind === "file") {
					let contents = await tg.blob(node.contents);
					let dependencies = Object.fromEntries(
						Object.entries(node.dependencies ?? {}).map(([key, value]) => {
							if (!value) {
								return [key, undefined];
							}
							let referent: tg.Referent<tg.Graph.Edge<tg.Object>>;
							if (
								typeof value === "number" ||
								"node" in value ||
								tg.Object.is(value)
							) {
								let item = tg.Graph.Edge.fromArg(value);
								referent = { item, options: {} };
							} else {
								let item = tg.Graph.Edge.fromArg(value.item);
								referent = { item, options: value.options };
							}
							return [key, referent];
						}),
					);
					let executable = node.executable ?? false;
					return {
						kind: "file" as const,
						contents,
						dependencies,
						executable,
					};
				} else if (node.kind === "symlink") {
					let artifact = tg.Graph.Edge.fromArg(node.artifact);
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
									"node" in entry &&
									entry.graph === undefined
								) {
									entry.node += offset;
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
								let referent: tg.Referent<tg.Graph.Arg.Edge<tg.Object>>;
								if (
									typeof value === "number" ||
									"node" in value ||
									tg.Object.is(value)
								) {
									referent = { item: value, options: {} };
								} else {
									referent = value;
								}
								if (typeof referent.item === "number") {
									node.dependencies[reference] = {
										item: referent.item + offset,
										options: referent.options,
									};
								} else if ("node" in referent.item) {
									node.dependencies[reference] = {
										item: {
											graph: referent.item.graph,
											node: referent.item.node + offset,
										},
										options: referent.options,
									};
								} else if (tg.Object.is(referent.item)) {
									node.dependencies[reference] = referent;
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
					let artifact: tg.Graph.Edge<tg.Artifact> | undefined;
					if (typeof argNode.artifact === "number") {
						artifact = { graph: undefined, node: argNode.artifact + offset };
					} else if (
						argNode.artifact !== undefined &&
						"node" in argNode.artifact
					) {
						artifact = {
							graph: argNode.artifact.graph,
							node: argNode.artifact.node + offset,
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
		if (this.#state.id! !== undefined) {
			return this.#state.id;
		}
		let object = this.#state.object!;
		let data = tg.Graph.Object.toData(object);
		let id = tg.handle.objectId({ kind: "graph", value: data });
		this.#state.id = id;
		return id;
	}

	async object(): Promise<tg.Graph.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load(): Promise<tg.Graph.Object> {
		if (this.#state.object === undefined) {
			let data = await tg.handle.getObject(this.#state.id!);
			tg.assert(data.kind === "graph");
			let object = tg.Graph.Object.fromData(data.value);
			this.#state.object = object;
		}
		return this.#state.object!;
	}

	async store(): Promise<tg.Graph.Id> {
		await tg.Value.store(this);
		return this.id;
	}

	async children(): Promise<Array<tg.Object>> {
		let object = await this.load();
		return tg.Graph.Object.children(object);
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
				return tg.Directory.withObject({ graph: this, node: index });
			}
			case "file": {
				return tg.File.withObject({ graph: this, node: index });
			}
			case "symlink": {
				return tg.Symlink.withObject({ graph: this, node: index });
			}
		}
	}
}

export namespace Graph {
	export type Id = string;

	export type State = tg.Object.State<tg.Graph.Id, tg.Graph.Object>;

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

		export type File = {
			contents?: tg.Blob.Arg | undefined;
			dependencies?:
				| {
						[reference: tg.Reference]:
							| tg.MaybeReferent<tg.Graph.Arg.Edge<tg.Object>>
							| undefined;
				  }
				| undefined;
			executable?: boolean | undefined;
		};

		export type Symlink = {
			artifact?: tg.Graph.Arg.Edge<tg.Artifact> | undefined;
			path?: string | undefined;
		};

		export type Edge<T> = tg.Graph.Arg.Reference | T;

		export type Reference =
			| number
			| {
					graph?: tg.Graph | undefined;
					node: number;
			  };

		export namespace Reference {
			export let is = (value: unknown): value is tg.Graph.Arg.Reference => {
				return (
					typeof value === "number" ||
					(typeof value === "object" &&
						value !== null &&
						"node" in value &&
						typeof value.node === "number")
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
			[reference: tg.Reference]:
				| tg.Referent<tg.Graph.Edge<tg.Object>>
				| undefined;
		};
		executable: boolean;
	};

	export namespace File {
		export let toData = (object: tg.Graph.File): tg.Graph.Data.File => {
			let data: tg.Graph.Data.File = {};
			data.contents = object.contents.id;
			if (globalThis.Object.entries(object.dependencies).length > 0) {
				data.dependencies = globalThis.Object.fromEntries(
					globalThis.Object.entries(object.dependencies).map(
						([reference, referent]) => {
							if (!referent) {
								return [reference, undefined];
							}
							return [
								reference,
								tg.Referent.toData(referent, (item) =>
									tg.Graph.Edge.toData(item, (item) => item.id),
								),
							];
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
						([reference, referent]) => {
							if (!referent) {
								return [reference, undefined];
							}
							return [
								reference,
								tg.Referent.fromData(referent, (item) =>
									tg.Graph.Edge.fromData(item, tg.Object.withId),
								),
							];
						},
					),
				),
				executable: data.executable ?? false,
			};
		};

		export let children = (object: tg.Graph.File): Array<tg.Object> => {
			let dependencies = globalThis.Object.entries(object.dependencies)
				.filter(([_, referent]) => referent !== undefined)
				.flatMap(([_, referent]) => tg.Graph.Edge.children(referent!.item));
			return [object.contents, ...dependencies];
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

	export type Edge<T> = tg.Graph.Reference | T;

	export namespace Edge {
		export let fromArg = <T>(arg: tg.Graph.Arg.Edge<T>): tg.Graph.Edge<T> => {
			if (tg.Graph.Arg.Reference.is(arg)) {
				return tg.Graph.Reference.fromArg(arg);
			} else {
				return arg;
			}
		};

		export let toData = <T, U>(
			object: tg.Graph.Edge<T>,
			f: (item: T) => U,
		): tg.Graph.Data.Edge<U> => {
			if (typeof object === "object" && object !== null && "node" in object) {
				return tg.Graph.Reference.toData(object);
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
				(typeof data === "object" && data !== null && "node" in data)
			) {
				try {
					return tg.Graph.Reference.fromData(data);
				} catch {}
			}
			return f(data as T);
		};

		export let toDataString = <T, U extends string>(
			object: tg.Graph.Edge<T>,
			f: (item: T) => U,
		): string => {
			if (typeof object === "object" && object !== null && "node" in object) {
				return tg.Graph.Reference.toDataString(object);
			} else {
				return f(object);
			}
		};

		export let children = <T>(object: tg.Graph.Edge<T>): Array<tg.Object> => {
			if (typeof object === "number") {
				return [];
			} else if (
				typeof object === "object" &&
				object !== null &&
				"node" in object
			) {
				return tg.Graph.Reference.children(object);
			} else if (tg.Object.is(object)) {
				return [object];
			} else {
				return [];
			}
		};
	}

	export type Reference = {
		graph?: tg.Graph | undefined;
		node: number;
	};

	export namespace Reference {
		export let fromArg = (arg: tg.Graph.Arg.Reference): tg.Graph.Reference => {
			return typeof arg === "number" ? { node: arg } : arg;
		};

		export let toData = (
			object: tg.Graph.Reference,
		): tg.Graph.Data.Reference => {
			let data: { graph?: tg.Graph.Id; node: number };
			if (object.graph !== undefined) {
				data = { graph: object.graph.id, node: object.node };
			} else {
				data = { node: object.node };
			}
			return data;
		};

		export let fromData = (
			data: tg.Graph.Data.Reference,
		): tg.Graph.Reference => {
			if (typeof data === "number") {
				return { graph: undefined, node: data };
			} else if (typeof data === "string") {
				return tg.Graph.Reference.fromDataString(data);
			} else {
				let graph = data.graph ? tg.Graph.withId(data.graph) : undefined;
				return { graph, node: data.node };
			}
		};

		export let toDataString = (object: tg.Graph.Reference): string => {
			let string = "";
			if (object.graph !== undefined) {
				string += `graph=${object.graph.id}&`;
			}
			string += `node=${object.node}`;
			return string;
		};

		export let fromDataString = (data: string): tg.Graph.Reference => {
			let graph: tg.Graph | undefined;
			let node: number | undefined;
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
					case "node": {
						node = Number.parseInt(decodeURIComponent(value), 10);
						break;
					}
					default: {
						throw new Error("invalid key");
					}
				}
			}
			tg.assert(node !== undefined);
			return { graph, node };
		};

		export let children = (object: tg.Graph.Reference): Array<tg.Object> => {
			if (object.graph !== undefined) {
				return [object.graph];
			} else {
				return [];
			}
		};

		export let is = (value: unknown): value is tg.Graph.Reference => {
			return (
				typeof value === "object" &&
				value !== null &&
				"node" in value &&
				typeof value.node === "number"
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
				[reference: tg.Reference]:
					| tg.Referent.Data<tg.Graph.Data.Edge<tg.Object.Id>>
					| undefined;
			};
			executable?: boolean;
		};

		export type Symlink = {
			artifact?: tg.Graph.Data.Edge<tg.Artifact.Id>;
			path?: string;
		};

		export type Edge<T> = tg.Graph.Data.Reference | T;

		export type Reference =
			| string
			| {
					graph?: tg.Graph.Id;
					node: number;
			  };

		export namespace Reference {
			export let is = (value: unknown): value is tg.Graph.Data.Reference => {
				return (
					typeof value === "number" ||
					typeof value === "string" ||
					(typeof value === "object" &&
						value !== null &&
						"node" in value &&
						typeof value.node === "number")
				);
			};
		}
	}
}
