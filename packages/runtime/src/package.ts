import { Args } from "./args.ts";
import { assert as assert_ } from "./assert.ts";
import type { Object_ as Object__ } from "./object.ts";
import { resolve } from "./resolve.ts";
import { flatten } from "./util.ts";
import type { Value } from "./value.ts";

export let package_ = async (...args: Args<Package.Arg>): Promise<Package> => {
	return await Package.new(...args);
};

export class Package {
	#state: Package.State;

	constructor(state: Package.State) {
		this.#state = state;
	}

	get state(): Package.State {
		return this.#state;
	}

	static withId(id: Package.Id): Package {
		return new Package({ id });
	}

	static async new(...args: Args<Package.Arg>): Promise<Package> {
		let arg = await Package.arg(...args);
		let nodes = await Promise.all(
			(arg.nodes ?? []).map(async (node) => {
				let dependencies: { [dependency: string]: Package | number } = {};
				let keys = Object.keys(node.dependencies ?? {});
				for (let key of keys) {
					let dependency = node.dependencies![key];
					if (dependency) {
						if (typeof dependency === "number") {
							dependencies[key] = dependency;
						} else {
							dependencies[key] = await Package.new(dependency);
						}
					}
				}
				let metadata = node.metadata ?? {};
				let object = node.object;
				return { dependencies, metadata, object };
			}),
		);
		let root = arg.root ?? 0;
		return new Package({ object: { nodes, root } });
	}

	static async arg(...args: Args<Package.Arg>): Promise<Package.ArgObject> {
		let resolved = await Promise.all(args.map(resolve));
		let flattened = flatten(resolved);
		let objects = await Promise.all(
			flattened.map(async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (arg instanceof Package) {
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

	static expect(value: unknown): Package {
		assert_(value instanceof Package);
		return value;
	}

	static assert(value: unknown): asserts value is Package {
		assert_(value instanceof Package);
	}

	async id(): Promise<Package.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<Package.Object_> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("load", this.#state.id!);
			assert_(object.kind === "package");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("store", {
				kind: "package",
				value: this.#state.object!,
			});
		}
	}

	async root(): Promise<number> {
		return (await this.object()).root;
	}

	async nodes(): Promise<Array<Package.Node>> {
		return (await this.object()).nodes;
	}
}

export namespace Package {
	export type Arg = Package | ArgObject;

	export type ArgObject = {
		nodes?: Array<NodeArg> | undefined;
		root?: number | undefined;
	};

	export type NodeArg = {
		dependencies?: { [dependency: string]: Package.Arg | number };
		metadata?: { [key: string]: Value };
		object?: Object__ | undefined;
	};

	export type Id = string;

	export type Object_ = {
		nodes: Array<Node>;
		root: number;
	};

	export type Node = {
		dependencies: { [dependency: string]: Package | number };
		metadata: { [key: string]: Value };
		object: Object__ | undefined;
	};

	export type State = Object__.State<Package.Id, Package.Object_>;
}
