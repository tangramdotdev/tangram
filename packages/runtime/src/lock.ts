import { Args } from "./args.ts";
import { assert as assert_ } from "./assert.ts";
import type { Object_ } from "./object.ts";
import { resolve } from "./resolve.ts";
import { flatten } from "./util.ts";

export let lock = async (...args: Args<Lock.Arg>): Promise<Lock> => {
	return await Lock.new(...args);
};

export class Lock {
	#state: Lock.State;

	constructor(state: Lock.State) {
		this.#state = state;
	}

	get state(): Lock.State {
		return this.#state;
	}

	static withId(id: Lock.Id): Lock {
		return new Lock({ id });
	}

	static async new(...args: Args<Lock.Arg>): Promise<Lock> {
		let arg = await Lock.arg(...args);
		let root = arg.root ?? 0;
		let nodes = await Promise.all(
			(arg.nodes ?? []).map(async (node) => {
				let keys = Object.keys(node.dependencies ?? {});
				let dependencies: { [dependency: string]: Lock | number } = {};
				for (let key of keys) {
					let dependency = node.dependencies![key];
					if (dependency) {
						if (typeof dependency === "number") {
							dependencies[key] = dependency;
						} else {
							dependencies[key] = await Lock.new(dependency);
						}
					}
				}
				return { dependencies };
			}),
		);
		return new Lock({ object: { root, nodes } });
	}

	static async arg(...args: Args<Lock.Arg>): Promise<Lock.ArgObject> {
		let resolved = await Promise.all(args.map(resolve));
		let flattened = flatten(resolved);
		let objects = await Promise.all(
			flattened.map(async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (arg instanceof Lock) {
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

	static expect(value: unknown): Lock {
		assert_(value instanceof Lock);
		return value;
	}

	static assert(value: unknown): asserts value is Lock {
		assert_(value instanceof Lock);
	}

	async id(): Promise<Lock.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<Lock.Object_> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("load", this.#state.id!);
			assert_(object.kind === "lock");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("store", {
				kind: "lock",
				value: this.#state.object!,
			});
		}
	}

	async root(): Promise<number> {
		return (await this.object()).root;
	}

	async nodes(): Promise<Array<Lock.Node>> {
		return (await this.object()).nodes;
	}
}

export namespace Lock {
	export type Arg = Lock | ArgObject;

	export type ArgObject = {
		root?: number;
		nodes?: Array<NodeArg>;
	};

	export type NodeArg = {
		dependencies?: { [dependency: string]: Lock.Arg | number };
	};

	export type Id = string;

	export type Object_ = {
		root: number;
		nodes: Array<Node>;
	};

	export type Node = {
		dependencies: { [dependency: string]: Lock | number };
	};

	export type State = Object_.State<Lock.Id, Lock.Object_>;
}
