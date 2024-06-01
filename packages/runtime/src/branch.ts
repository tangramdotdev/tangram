import { Args } from "./args.ts";
import { assert as assert_ } from "./assert.ts";
import type { Blob } from "./blob.ts";
import * as encoding from "./encoding.ts";
import type { Object_ } from "./object.ts";
import { resolve } from "./resolve.ts";
import { flatten } from "./util.ts";

export let branch = async (...args: Args<Branch.Arg>): Promise<Branch> => {
	return await Branch.new(...args);
};

export class Branch {
	#state: Branch.State;

	constructor(state: Branch.State) {
		this.#state = state;
	}

	get state(): Branch.State {
		return this.#state;
	}

	static withId(id: Branch.Id): Branch {
		return new Branch({ id });
	}

	static async new(...args: Args<Branch.Arg>): Promise<Branch> {
		let arg = await Branch.arg(...args);
		let children = arg.children ?? [];
		let object = { children };
		return new Branch({ object });
	}

	static async arg(...args: Args<Branch.Arg>): Promise<Branch.ArgObject> {
		let resolved = await Promise.all(args.map(resolve));
		let flattened = flatten(resolved);
		let objects = await Promise.all(
			flattened.map(async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (arg instanceof Branch) {
					let child = { blob: arg, size: await arg.size() };
					return {
						children: [child],
					};
				} else {
					return arg;
				}
			}),
		);
		let mutations = await Args.createMutations(objects, {
			children: "append",
		});
		let arg = await Args.applyMutations(mutations);
		return arg;
	}

	static expect(value: unknown): Branch {
		assert_(value instanceof Branch);
		return value;
	}

	static assert(value: unknown): asserts value is Branch {
		assert_(value instanceof Branch);
	}

	async id(): Promise<Branch.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<Branch.Object_> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("load", this.#state.id!);
			assert_(object.kind === "branch");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("store", {
				kind: "branch",
				value: this.#state.object!,
			});
		}
	}

	async children(): Promise<Array<Branch.Child>> {
		return (await this.object()).children;
	}

	async size(): Promise<number> {
		return (await this.children())
			.map(({ size }) => size)
			.reduce((a, b) => a + b, 0);
	}

	async bytes(): Promise<Uint8Array> {
		return await syscall("read", this);
	}

	async text(): Promise<string> {
		return encoding.utf8.decode(await this.bytes());
	}
}

export namespace Branch {
	export type Arg = undefined | Branch | ArgObject;

	export type ArgObject = {
		children?: Array<Child>;
	};

	export type Child = { blob: Blob; size: number };

	export type Id = string;

	export type Object_ = { children: Array<Child> };

	export type State = Object_.State<Branch.Id, Branch.Object_>;
}
