import * as tg from "./index.ts";
import { flatten } from "./util.ts";

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
		return new Graph({ id });
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
						dependencies: node.dependencies ?? undefined,
						executable: node.executable ?? false,
					};
				} else if (node.kind === "symlink") {
					return {
						kind: "symlink" as const,
						artifact: node.artifact ?? undefined,
						path: node.path !== undefined ? tg.path(node.path) : undefined,
					};
				} else {
					return tg.unreachable(node);
				}
			})
		);
		return new Graph({ object: { nodes } });
	}

	static async arg(...args: tg.Args<Graph.Arg>): Promise<Graph.ArgObject> {
		let resolved = await Promise.all(args.map(tg.resolve));
		let flattened = flatten(resolved);
		let nodes = [];
		let offset = 0;
		for (let arg of flattened) {
			let argNodes = arg instanceof Graph ? await arg.nodes() : arg.nodes || [];
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
							if (Array.isArray(argNode.dependencies)) {
								node.dependencies = argNode.dependencies.map((dependency) =>
									typeof dependency === "number"
										? dependency + offset
										: dependency
								);
							} else {
								node.dependencies = {};
								for (let reference in argNode.dependencies) {
									if (typeof argNode.dependencies[reference] === "number") {
										node.dependencies[reference] =
											argNode.dependencies[reference] + offset;
									} else if (tg.Object.is(argNode.dependencies[reference])) {
										node.dependencies[reference] =
											argNode.dependencies[reference];
									}
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
					let node: tg.Graph.SymlinkNodeArg = {
						kind: "symlink" as const,
					};
					if ("artifact" in argNode) {
						if (
							argNode.artifact !== undefined &&
							typeof argNode.artifact === "number"
						) {
							node.artifact = argNode.artifact + offset;
						} else {
							node.artifact = argNode.artifact;
						}
					}
					if ("path" in argNode) {
						node.path = argNode.path;
					}
					nodes.push(node);
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

	async id(): Promise<Graph.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<Graph.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("load", this.#state.id!);
			tg.assert(object.kind === "graph");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("store", {
				kind: "graph",
				value: this.#state.object!,
			});
		}
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
			| Array<number | tg.Object>
			| { [reference: string]: number | tg.Object }
			| undefined;
		executable?: boolean | undefined;
	};

	export type SymlinkNodeArg = {
		kind: "symlink";
		artifact?: number | tg.Artifact | undefined;
		path?: tg.Path.Arg | undefined;
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
		dependencies:
			| Array<number | tg.Object>
			| { [reference: string]: number | tg.Object }
			| undefined;
		executable: boolean;
	};

	export type SymlinkNode = {
		kind: "symlink";
		artifact: number | tg.Artifact | undefined;
		path: tg.Path | undefined;
	};

	export type State = tg.Object.State<Graph.Id, Graph.Object>;
}
