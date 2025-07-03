import * as tg from "./index.ts";

export let graph = async (...args: tg.Args<Graph.Arg>): Promise<Graph> => {
	return await Graph.new(...args);
};

export class Graph {
	#state: Graph.State;

	constructor(state: Graph.State) {
		this.#state = state;
	}

	get state(): Graph.State {
		return this.#state;
	}

	static withId(id: Graph.Id): Graph {
		return new Graph({ id, stored: true });
	}

	static withObject(object: Graph.Object): Graph {
		return new Graph({ object, stored: false });
	}

	static fromData(data: Graph.Data): Graph {
		return Graph.withObject(Graph.Object.fromData(data));
	}

	static async new(...args: tg.Args<Graph.Arg>): Promise<Graph> {
		let arg = await Graph.arg(...args);
		let nodes = await Promise.all(
			(arg.nodes ?? []).map(async (node) => {
				if (node.kind === "directory") {
					return {
						kind: "directory" as const,
						entries: node.entries ?? {},
					};
				} else if (node.kind === "file") {
					return {
						kind: "file" as const,
						contents: await tg.blob(node.contents),
						dependencies: Object.fromEntries(
							Object.entries(node.dependencies ?? {}).map(([key, value]) => {
								if (
									tg.Object.is(value) ||
									typeof value === "number" ||
									"node" in value
								) {
									value = { item: value };
								}
								return [key, value];
							}),
						),
						executable: node.executable ?? false,
					};
				} else if (node.kind === "symlink") {
					return {
						kind: "symlink" as const,
						artifact: node.artifact,
						path: node.path,
					};
				} else {
					return tg.unreachable();
				}
			}),
		);
		return Graph.withObject({ nodes });
	}

	static async arg(...args: tg.Args<Graph.Arg>): Promise<Graph.ArgObject> {
		let resolved = await Promise.all(args.map(tg.resolve));
		let nodes = [];
		let offset = 0;
		for (let arg of resolved) {
			let argNodes =
				arg instanceof Graph
					? await arg.nodes()
					: Array.isArray(arg.nodes)
						? arg.nodes
						: [];
			for (let argNode of argNodes) {
				if (argNode.kind === "directory") {
					let node: tg.Graph.DirectoryNodeArg = { kind: "directory" as const };
					if ("entries" in argNode) {
						if (argNode.entries !== undefined) {
							node.entries = {};
							for (let name in argNode.entries) {
								let entry = argNode.entries[name];
								if (typeof entry === "number") {
									node.entries[name] = entry + offset;
								} else if (
									entry !== undefined &&
									"node" in entry &&
									!entry.graph
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
					let node: tg.Graph.FileNodeArg = {
						kind: "file" as const,
						contents: argNode.contents,
					};
					if ("dependencies" in argNode) {
						if (argNode.dependencies !== undefined) {
							node.dependencies = {};
							for (let reference in argNode.dependencies) {
								let referent: tg.Referent<tg.Graph.Object.Edge<tg.Object>>;
								let value = argNode.dependencies[reference]!;
								if (
									tg.Object.is(value) ||
									typeof value === "number" ||
									"node" in value
								) {
									referent = { item: value };
								} else {
									referent = value;
								}
								argNode.dependencies[reference];
								if (typeof referent.item === "number") {
									node.dependencies[reference] = {
										...referent,
										item: referent.item + offset,
									};
								} else if ("node" in referent.item) {
									node.dependencies[reference] = {
										...referent,
										item: {
											...referent.item,
											node: referent.item.node + offset,
										},
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
					let edge: tg.Graph.Object.Edge<tg.Artifact> | undefined;
					if (typeof argNode.artifact === "number") {
						edge = argNode.artifact + offset;
					} else if (
						argNode.artifact !== undefined &&
						"node" in argNode.artifact
					) {
						edge = {
							...argNode.artifact,
							node: argNode.artifact.node + offset,
						};
					} else {
						edge = argNode.artifact;
					}
					nodes.push({
						kind: "symlink" as const,
						artifact: edge,
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

	static expect(value: unknown): Graph {
		tg.assert(value instanceof Graph);
		return value;
	}

	static assert(value: unknown): asserts value is Graph {
		tg.assert(value instanceof Graph);
	}

	get id(): Graph.Id {
		if (this.#state.id! !== undefined) {
			return this.#state.id;
		}
		let object = this.#state.object!;
		let data = Graph.Object.toData(object);
		let id = syscall("object_id", { kind: "graph", value: data });
		this.#state.id = id;
		return id;
	}

	async get(index: number): Promise<tg.Artifact> {
		const node = (await this.nodes())[index];
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

	async object(): Promise<Graph.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load(): Promise<tg.Graph.Object> {
		if (this.#state.object === undefined) {
			let data = await syscall("object_get", this.#state.id!);
			tg.assert(data.kind === "graph");
			let object = Graph.Object.fromData(data.value);
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

	async nodes(): Promise<Array<Graph.Node>> {
		return (await this.object()).nodes;
	}
}

export namespace Graph {
	export type Id = string;

	export type Arg = Graph | ArgObject;

	export type ArgObject = {
		nodes?: Array<NodeArg> | undefined;
	};

	export type NodeArg = DirectoryNodeArg | FileNodeArg | SymlinkNodeArg;

	export type Ref = {
		graph?: Graph | undefined;
		node: number;
	};

	export type DirectoryNodeArg = {
		kind: "directory";
		entries?: { [name: string]: Object.Edge<tg.Artifact> } | undefined;
	};

	export type FileNodeArg = {
		kind: "file";
		contents: tg.Blob.Arg;
		dependencies?:
			| { [reference: tg.Reference]: tg.MaybeReferent<Object.Edge<tg.Object>> }
			| undefined;
		executable?: boolean | undefined;
	};

	export type SymlinkNodeArg = {
		kind: "symlink";
		artifact?: Object.Edge<tg.Artifact> | undefined;
		path?: string | undefined;
	};

	export type Object = {
		nodes: Array<Node>;
	};

	export type Node = DirectoryNode | FileNode | SymlinkNode;

	export type DirectoryNode = {
		kind: "directory";
		entries: { [name: string]: Object.Edge<tg.Artifact> };
	};

	export type FileNode = {
		kind: "file";
		contents: tg.Blob;
		dependencies: {
			[reference: tg.Reference]: tg.Referent<Object.Edge<tg.Object>>;
		};
		executable: boolean;
	};

	export type SymlinkNode = {
		kind: "symlink";
		artifact: Object.Edge<tg.Artifact> | undefined;
		path: string | undefined;
	};

	export namespace Object {
		export type Edge<T> = Ref | T | number;
		export type Ref = {
			graph?: tg.Graph | undefined;
			node: number;
		};
		export let toData = (object: Object): Data => {
			return {
				nodes: object.nodes.map(Node.toData),
			};
		};

		export let fromData = (data: Data): Object => {
			return {
				nodes: data.nodes.map(Node.fromData),
			};
		};

		export let children = (object: Object): Array<tg.Object> => {
			return object.nodes.flatMap(tg.Graph.Node.children);
		};
	}

	export namespace Data {
		export type Edge<T> = Ref | T | number;
		export type Ref = {
			graph?: tg.Graph.Id | undefined;
			node: number;
		};
	}

	export namespace Edge {
		export const toData = <T, U>(
			edge: Object.Edge<T>,
			f: (item: T) => U,
		): Data.Edge<U> => {
			tg.assert(edge !== null);
			if (typeof edge === "number") {
				return edge;
			}
			if (typeof edge === "object" && "node" in edge) {
				return { graph: edge.graph?.id, node: edge.node };
			}
			return f(edge);
		};

		export const fromData = <T, U>(
			edge: Data.Edge<T>,
			f: (item: T) => U,
		): Object.Edge<U> => {
			tg.assert(edge !== null);
			if (typeof edge === "number") {
				return edge;
			}
			if (typeof edge === "object" && "node" in edge) {
				const graph = edge.graph ? tg.Graph.withId(edge.graph) : undefined;
				return { graph, node: edge.node };
			}
			return f(edge);
		};
	}

	export namespace Node {
		export let toData = (object: Node): NodeData => {
			if (object.kind === "directory") {
				return {
					kind: "directory",
					entries: globalThis.Object.fromEntries(
						globalThis.Object.entries(object.entries).map(
							([name, artifact]) => [
								name,
								Edge.toData(artifact, (artifact) => artifact.id),
							],
						),
					),
				};
			} else if (object.kind === "file") {
				return {
					kind: "file",
					contents: object.contents.id,
					dependencies: globalThis.Object.fromEntries(
						globalThis.Object.entries(object.dependencies).map(
							([reference, referent]) => {
								return [
									reference,
									tg.Referent.toData(referent, (item) =>
										Edge.toData(item, (item) => item.id),
									),
								];
							},
						),
					),
					executable: object.executable,
				};
			} else if (object.kind === "symlink") {
				let output: SymlinkNodeData = {
					kind: "symlink",
				};
				if (object.artifact !== undefined) {
					output.artifact = Edge.toData(
						object.artifact,
						(artifact) => artifact.id,
					);
				}
				if (object.path !== undefined) {
					output.path = object.path;
				}
				return output;
			} else {
				throw new Error("invalid node node");
			}
		};

		export let fromData = (data: NodeData): Node => {
			if (data.kind === "directory") {
				return {
					kind: "directory",
					entries: globalThis.Object.fromEntries(
						globalThis.Object.entries(data.entries).map(([name, edge]) => [
							name,
							Edge.fromData(edge, tg.Artifact.withId),
						]),
					),
				};
			} else if (data.kind === "file") {
				return {
					kind: "file",
					contents: tg.Blob.withId(data.contents),
					dependencies: globalThis.Object.fromEntries(
						globalThis.Object.entries(data.dependencies ?? {}).map(
							([reference, referent]) => {
								return [
									reference,
									tg.Referent.fromData(referent, (item) =>
										Edge.fromData(item, tg.Object.withId),
									),
								];
							},
						),
					),
					executable: data.executable ?? false,
				};
			} else if (data.kind === "symlink") {
				return {
					kind: "symlink",
					artifact:
						data.artifact !== undefined
							? Edge.fromData(data.artifact, tg.Artifact.withId)
							: undefined,
					path: data.path,
				};
			} else {
				throw new Error("invalid node kind");
			}
		};

		export let children = (node: Node): Array<tg.Object> => {
			switch (node.kind) {
				case "directory": {
					return globalThis.Object.entries(node.entries)
						.filter(
							([_, edge]) =>
								(typeof edge === "object" &&
									"node" in edge &&
									edge.graph !== undefined) ||
								true,
						)
						.map(([_, edge]) => {
							tg.assert(typeof edge === "object", "expected an object");
							if ("node" in edge) {
								tg.assert(edge.graph !== undefined, "missing graph");
								return edge.graph;
							} else {
								return edge;
							}
						});
				}
				case "file": {
					return [
						node.contents,
						...globalThis.Object.entries(node.dependencies)
							.filter(
								([_, referent]) =>
									(typeof referent.item === "object" &&
										"node" in referent.item &&
										referent.item.graph !== undefined) ||
									true,
							)
							.map(([_, referent]) => {
								tg.assert(
									typeof referent.item === "object",
									"expected an object",
								);
								if ("node" in referent.item) {
									tg.assert(referent.item.graph !== undefined, "missing graph");
									return referent.item.graph;
								} else {
									return referent.item;
								}
							}),
					];
				}
				case "symlink": {
					if ("artifact" in node && node.artifact !== undefined) {
						tg.assert(typeof node.artifact === "object", "expected an object");
						if ("node" in node.artifact) {
							if (node.artifact.graph) {
								return [node.artifact.graph];
							}
						} else {
							return [node.artifact];
						}
					}
					return [];
				}
			}
		};
	}

	export type Data = {
		nodes: Array<NodeData>;
	};

	export type NodeData = DirectoryNodeData | FileNodeData | SymlinkNodeData;

	export type DirectoryNodeData = {
		kind: "directory";
		entries: { [name: string]: Data.Edge<tg.Artifact.Id> };
	};

	export type FileNodeData = {
		kind: "file";
		contents: tg.Blob.Id;
		dependencies?: {
			[reference: tg.Reference]: tg.Referent.Data<Data.Edge<tg.Object.Id>>;
		};
		executable?: boolean;
	};

	export type SymlinkNodeData = {
		kind: "symlink";
		artifact?: Data.Edge<tg.Artifact.Id>;
		path?: string;
	};

	export type State = tg.Object.State<Graph.Id, Graph.Object>;
}
