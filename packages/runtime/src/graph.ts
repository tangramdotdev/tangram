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
						dependencies: node.dependencies ?? {},
						executable: node.executable ?? false,
					};
				} else if (node.kind === "symlink") {
					if ("target" in node) {
						return {
							kind: "symlink" as const,
							target: node.target,
						};
					} else if ("artifact" in node) {
						return {
							kind: "symlink" as const,
							artifact: node.artifact ?? undefined,
							subpath: node.subpath !== undefined ? node.subpath : undefined,
						};
					} else {
						return tg.unreachable();
					}
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
								if (typeof argNode.entries[name] === "number") {
									node.entries[name] = argNode.entries[name] + offset;
								} else if (tg.Artifact.is(argNode.entries[name])) {
									node.entries[name] = argNode.entries[name];
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
								if (typeof argNode.dependencies[reference]?.item === "number") {
									node.dependencies[reference] = {
										...argNode.dependencies[reference],
										item: argNode.dependencies[reference].item + offset,
									};
								} else if (
									tg.Object.is(argNode.dependencies[reference]?.item)
								) {
									node.dependencies[reference] =
										argNode.dependencies[reference];
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
					if ("target" in argNode) {
						nodes.push({
							kind: "symlink" as const,
							target: argNode.target,
						});
					} else if ("artifact" in argNode) {
						let artifact: number | tg.Artifact;
						if (typeof argNode.artifact === "number") {
							artifact = argNode.artifact + offset;
						} else {
							artifact = argNode.artifact;
						}
						nodes.push({
							kind: "symlink" as const,
							artifact,
							subpath: argNode.subpath,
						});
					} else {
						return tg.unreachable();
					}
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

	export type DirectoryNodeArg = {
		kind: "directory";
		entries?: { [name: string]: number | tg.Artifact } | undefined;
	};

	export type FileNodeArg = {
		kind: "file";
		contents: tg.Blob.Arg;
		dependencies?:
			| { [reference: tg.Reference]: tg.Referent<number | tg.Object> }
			| undefined;
		executable?: boolean | undefined;
	};

	export type SymlinkNodeArg =
		| {
				kind: "symlink";
				target: string;
		  }
		| {
				kind: "symlink";
				artifact: number | tg.Artifact;
				subpath?: string | undefined;
		  };

	export type Object = {
		nodes: Array<Node>;
	};

	export type Node = DirectoryNode | FileNode | SymlinkNode;

	export type DirectoryNode = {
		kind: "directory";
		entries: { [name: string]: number | tg.Artifact };
	};

	export type FileNode = {
		kind: "file";
		contents: tg.Blob;
		dependencies: {
			[reference: tg.Reference]: tg.Referent<number | tg.Object>;
		};
		executable: boolean;
	};

	export type SymlinkNode =
		| {
				kind: "symlink";
				target: string;
		  }
		| {
				kind: "symlink";
				artifact: number | tg.Artifact;
				subpath: string | undefined;
		  };

	export namespace Object {
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

	export namespace Node {
		export let toData = (object: Node): NodeData => {
			if (object.kind === "directory") {
				return {
					kind: "directory",
					entries: globalThis.Object.fromEntries(
						globalThis.Object.entries(object.entries).map(
							([name, artifact]) => [
								name,
								typeof artifact === "number" ? artifact : artifact.id,
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
							([reference, referent]) => [
								reference,
								{
									...referent,
									item:
										typeof referent.item === "number"
											? referent.item
											: referent.item.id,
								},
							],
						),
					),
					executable: object.executable,
				};
			} else if (object.kind === "symlink") {
				if ("target" in object) {
					return {
						kind: "symlink",
						target: object.target,
					};
				} else {
					let output: SymlinkNodeData = {
						kind: "symlink",
						artifact:
							typeof object.artifact === "number"
								? object.artifact
								: object.artifact.id,
					};
					if (object.subpath !== undefined) {
						output.subpath = object.subpath;
					}
					return output;
				}
			} else {
				throw new Error("invalid node node");
			}
		};

		export let fromData = (data: NodeData): Node => {
			if (data.kind === "directory") {
				return {
					kind: "directory",
					entries: globalThis.Object.fromEntries(
						globalThis.Object.entries(data.entries).map(([name, artifact]) => [
							name,
							typeof artifact === "number"
								? artifact
								: tg.Artifact.withId(artifact),
						]),
					),
				};
			} else if (data.kind === "file") {
				return {
					kind: "file",
					contents: tg.Blob.withId(data.contents),
					dependencies: globalThis.Object.fromEntries(
						globalThis.Object.entries(data.dependencies ?? {}).map(
							([reference, referent]) => [
								reference,
								{
									...referent,
									item:
										typeof referent.item === "number"
											? referent.item
											: tg.Object.withId(referent.item),
								},
							],
						),
					),
					executable: data.executable ?? false,
				};
			} else if (data.kind === "symlink") {
				if ("target" in data) {
					return {
						kind: "symlink",
						target: data.target,
					};
				} else {
					return {
						kind: "symlink",
						artifact:
							typeof data.artifact === "number"
								? data.artifact
								: tg.Artifact.withId(data.artifact),
						subpath: data.subpath,
					};
				}
			} else {
				throw new Error("invalid node kind");
			}
		};

		export let children = (node: Node): Array<tg.Object> => {
			switch (node.kind) {
				case "directory": {
					return globalThis.Object.entries(node.entries)
						.map(([_, artifact]) => artifact)
						.filter((object) => typeof object !== "number");
				}
				case "file": {
					return [
						node.contents,
						...globalThis.Object.entries(node.dependencies)
							.map(([_, referent]) => referent.item)
							.filter((object) => typeof object !== "number"),
					];
				}
				case "symlink": {
					if ("artifact" in node && typeof node.artifact !== "number") {
						return [node.artifact];
					} else {
						return [];
					}
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
		entries: { [name: string]: number | tg.Artifact.Id };
	};

	export type FileNodeData = {
		kind: "file";
		contents: tg.Blob.Id;
		dependencies?: {
			[reference: tg.Reference]: tg.Referent<number | tg.Object.Id>;
		};
		executable?: boolean;
	};

	export type SymlinkNodeData =
		| {
				kind: "symlink";
				target: string;
		  }
		| {
				kind: "symlink";
				artifact: number | tg.Artifact.Id;
				subpath?: string;
		  };

	export type State = tg.Object.State<Graph.Id, Graph.Object>;
}
