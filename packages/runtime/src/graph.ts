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
		// Combine incoming arguments.
		let arg = await Graph.arg(...args);

		// Make all optional argument fields explicit.
		let nodes: Array<Graph.Node> = await Promise.all(
			(arg.nodes ?? []).map(async (node) => {
				if (node.kind === "directory") {
					return {
						kind: "directory",
						entries: node.entries ?? {},
					};
				} else if (node.kind === "file") {
					return {
						kind: "file",
						contents: await tg.blob(node.contents),
						dependencies: node.dependencies ?? undefined,
						executable: node.executable ?? false,
					};
				} else if (node.kind === "symlink") {
					return {
						kind: "symlink",
						artifact: node.artifact ?? undefined,
						path: node.path ? tg.path(node.path) : undefined,
					};
				} else {
					return tg.unreachable(node);
				}
			})
		);

		// Construct the object and return a new instance.
		return new Graph({ object: { nodes } });
	}

	static async arg(...args: tg.Args<Graph.Arg>): Promise<Graph.ArgObject> {
		// Resolve and flatten the incoming arguments.
		let resolved = await Promise.all(args.map(tg.resolve));
		let flattened = flatten(resolved);

		// Add the given offset to all indices in the object.
		let addOffset = (obj: any, offset: number): any => {
			if (typeof obj === "number") return obj + offset;
			if (Array.isArray(obj)) return obj.map((item) => addOffset(item, offset));
			if (obj && typeof obj === "object") {
				return Object.fromEntries(
					Object.entries(obj).map(([key, value]) => [
						key,
						addOffset(value, offset),
					])
				);
			}
			return obj;
		};

		// Process all arguments, renumbering all indices.
		let nodes: Array<Graph.NodeArg> = [];
		let offset = 0;

		for (let arg of flattened) {
			let argNodes =
				arg instanceof Graph
					? ((await arg.nodes()) as Array<Graph.NodeArg>)
					: arg.nodes || [];
			let renumberedNodes = argNodes.map((node) => addOffset(node, offset));
			nodes.push(...renumberedNodes);
			offset += renumberedNodes.length;
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
		entries?: { [name: string]: number | tg.Artifact };
	};

	export type FileNodeArg = {
		kind: "file";
		contents: tg.Blob.Arg;
		dependencies?:
			| Array<number | tg.Object>
			| { [reference: string]: number | tg.Object }
			| undefined;
		executable?: boolean;
	};

	export type SymlinkNodeArg = {
		kind: "symlink";
		artifact?: tg.Artifact | undefined;
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
		artifact: tg.Artifact | undefined;
		path: tg.Path | undefined;
	};

	export type State = tg.Object.State<Graph.Id, Graph.Object>;
}
