import { Args } from "./args.ts";
import { assert as assert_, unreachable } from "./assert.ts";
import { Mutation } from "./mutation.ts";
import type { Object_ } from "./object.ts";
import { type Unresolved, resolve } from "./resolve.ts";
import type {
	MaybeMutationMap,
	MaybeNestedArray,
	MutationMap,
} from "./util.ts";

export let lock = async (
	...args: Array<Unresolved<MaybeNestedArray<MaybeMutationMap<Lock.Arg>>>>
): Promise<Lock> => {
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

	static async new(
		...args: Array<Unresolved<MaybeNestedArray<MaybeMutationMap<Lock.Arg>>>>
	): Promise<Lock> {
		type Apply = {
			root: number;
			nodeArgs: Array<Lock.NodeArg>;
		};
		let { root, nodeArgs } = await Args.apply<Lock.Arg, Apply>(
			await Promise.all(args.map(resolve)),
			async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (Lock.is(arg)) {
					return {
						root: await arg.root(),
						nodes: await Mutation.arrayAppend(await arg.nodes()),
					};
				} else if (typeof arg === "object") {
					let object: MutationMap<Apply> = {};
					if (arg.root !== undefined) {
						object.root = arg.root;
					}
					if (arg.nodes !== undefined) {
						object.nodeArgs = Mutation.is(arg.nodes)
							? arg.nodes
							: await Mutation.arrayAppend(arg.nodes);
					}
					return object;
				} else {
					return unreachable();
				}
			},
		);
		root ??= 0;
		nodeArgs ??= [];
		let nodes = await Promise.all(
			nodeArgs.map(async (argObject) => {
				let dependenciesKeys = Object.keys(argObject.dependencies ?? {});
				let dependencies: { [dependency: string]: Lock } = {};
				for (let key of dependenciesKeys) {
					let dependency = argObject.dependencies![key];
					if (dependency) {
						dependencies[key] = await Lock.new(dependency);
					}
				}
				return { dependencies };
			}),
		);
		return new Lock({ object: { root, nodes } });
	}

	static is(value: unknown): value is Lock {
		return value instanceof Lock;
	}

	static expect(value: unknown): Lock {
		assert_(Lock.is(value));
		return value;
	}

	static assert(value: unknown): asserts value is Lock {
		assert_(Lock.is(value));
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
			let object = await syscall("object_load", this.#state.id!);
			assert_(object.kind === "lock");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("object_store", {
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
		dependencies?: { [dependency: string]: Lock.Arg };
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
