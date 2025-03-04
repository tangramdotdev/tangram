import * as tg from "./index.ts";
import { flatten } from "./util.ts";

export let branch = async (...args: tg.Args<Branch.Arg>): Promise<Branch> => {
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

	static async new(...args: tg.Args<Branch.Arg>): Promise<Branch> {
		let arg = await Branch.arg(...args);
		let children = arg.children ?? [];
		let object = { children };
		return new Branch({ object });
	}

	static async arg(...args: tg.Args<Branch.Arg>): Promise<Branch.ArgObject> {
		let resolved = await Promise.all(args.map(tg.resolve));
		let flattened = flatten(resolved);
		let objects = await Promise.all(
			flattened.map(async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (arg instanceof Branch) {
					let child = { blob: arg, size: await arg.length() };
					return {
						children: [child],
					};
				} else {
					return arg;
				}
			}),
		);
		let mutations = await tg.Args.createMutations(objects, {
			children: "append",
		});
		let arg = await tg.Args.applyMutations(mutations);
		return arg;
	}

	static expect(value: unknown): Branch {
		tg.assert(value instanceof Branch);
		return value;
	}

	static assert(value: unknown): asserts value is Branch {
		tg.assert(value instanceof Branch);
	}

	async id(): Promise<Branch.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<Branch.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("object_load", this.#state.id!);
			tg.assert(object.kind === "branch");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("object_store", {
				kind: "branch",
				value: this.#state.object!,
			});
		}
	}

	async children(): Promise<Array<Branch.Child>> {
		return (await this.object()).children;
	}

	async length(): Promise<number> {
		return (await this.children())
			.map(({ length }) => length)
			.reduce((a, b) => a + b, 0);
	}

	async bytes(): Promise<Uint8Array> {
		return await syscall("blob_read", this);
	}

	async text(): Promise<string> {
		return tg.encoding.utf8.decode(await this.bytes());
	}
}

export namespace Branch {
	export type Arg = undefined | Branch | ArgObject;

	export type ArgObject = {
		children?: Array<Child> | undefined;
	};

	export type Child = { blob: tg.Blob; length: number };

	export type Id = string;

	export type Object = { children: Array<Child> };

	export type State = tg.Object.State<Branch.Id, Branch.Object>;
}
