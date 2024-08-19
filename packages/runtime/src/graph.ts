import { Args } from "./args.ts";
import { assert as assert_ } from "./assert.ts";
import type { Object_ as Object__ } from "./object.ts";
import { resolve } from "./resolve.ts";
import { flatten } from "./util.ts";

export let graph = async (...args: Args<Graph.Arg>): Promise<Graph> => {
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

	static async new(...args: Args<Graph.Arg>): Promise<Graph> {
		let arg = await Graph.arg(...args);
		let nodes = await Promise.all(
			(arg.nodes ?? []).map(async (node) => {
				let dependencies: { [reference: string]: Graph | number } = {};
				let keys = Object.keys(node.dependencies ?? {});
				for (let key of keys) {
					let dependency = node.dependencies![key];
					if (dependency) {
						if (typeof dependency === "number") {
							dependencies[key] = dependency;
						} else {
							dependencies[key] = await Graph.new(dependency);
						}
					}
				}
				let object = node.object;
				return { dependencies, object };
			}),
		);
		return new Graph({ object: { nodes } });
	}

	static async arg(...args: Args<Graph.Arg>): Promise<Graph.ArgObject> {
		let resolved = await Promise.all(args.map(resolve));
		let flattened = flatten(resolved);
		let objects = await Promise.all(
			flattened.map(async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (arg instanceof Graph) {
					return arg.object();
				} else {
					return arg;
				}
			}),
		);
		let mutations = await Args.createMutations(objects, {
			nodes: "append",
		});
		let arg = await Args.applyMutations(mutations);
		return arg;
	}

	static expect(value: unknown): Graph {
		assert_(value instanceof Graph);
		return value;
	}

	static assert(value: unknown): asserts value is Graph {
		assert_(value instanceof Graph);
	}

	async id(): Promise<Graph.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<Graph.Object_> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("load", this.#state.id!);
			assert_(object.kind === "graph");
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
	export type Arg = Graph | ArgObject;

	export type ArgObject = {
		nodes?: Array<NodeArg> | undefined;
	};

	export type NodeArg = {
		dependencies?: { [reference: string]: number | Graph.Arg };
		object?: Object__ | undefined;
	};

	export type Id = string;

	export type Object_ = {
		nodes: Array<Node>;
	};

	export type Node = {
		dependencies: { [reference: string]: number | Graph };
	};

	export type State = Object__.State<Graph.Id, Graph.Object_>;
}
