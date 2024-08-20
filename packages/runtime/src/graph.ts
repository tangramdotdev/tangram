import { todo } from "./assert.ts";
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
		return new Graph({ id });
	}

	static async new(..._args: tg.Args<Graph.Arg>): Promise<Graph> {
		return todo();
	}

	static async arg(..._args: tg.Args<Graph.Arg>): Promise<Graph.ArgObject> {
		return todo();
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
			| Array<number | Object>
			| { [reference: string]: number | Object }
			| undefined;
		executable: boolean;
	};

	export type SymlinkNode = {
		kind: "symlink";
		artifact: tg.Artifact | undefined;
		path: tg.Path | undefined;
	};

	export type State = tg.Object.State<Graph.Id, Object>;
}
