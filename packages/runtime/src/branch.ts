import { Args } from "./args.ts";
import { Artifact } from "./artifact.ts";
import { assert as assert_, unreachable } from "./assert.ts";
import { Blob } from "./blob.ts";
import * as encoding from "./encoding.ts";
import { Mutation, mutation } from "./mutation.ts";
import { Object_ } from "./object.ts";
import * as syscall from "./syscall.ts";
import { MutationMap } from "./util.ts";

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
		type Apply = {
			children: Array<Branch.Child>;
		};
		let { children } = await Args.apply<Branch.Arg, Apply>(
			args,
			async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (Branch.is(arg)) {
					return {
						children: await mutation({
							kind: "array_append",
							values: [{ blob: arg, size: await arg.size() }],
						}),
					};
				} else if (typeof arg === "object") {
					let object: MutationMap<Apply> = {};
					if (arg.children !== undefined) {
						object.children = Mutation.is(arg.children)
							? arg.children
							: await mutation({
									kind: "array_append",
									values: arg.children ?? [],
								});
					}
					return object;
				} else {
					return unreachable();
				}
			},
		);
		children ??= [];
		return new Branch({ object: { children } });
	}

	static is(value: unknown): value is Branch {
		return value instanceof Branch;
	}

	static expect(value: unknown): Branch {
		assert_(Branch.is(value));
		return value;
	}

	static assert(value: unknown): asserts value is Branch {
		assert_(Branch.is(value));
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
			let object = await syscall.load(this.#state.id!);
			assert_(object.kind === "branch");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall.store({
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
		return await syscall.read(this);
	}

	async text(): Promise<string> {
		return encoding.utf8.decode(await syscall.read(this));
	}

	async compress(format: Blob.CompressionFormat): Promise<Blob> {
		return await syscall.compress(this, format);
	}

	async decompress(format: Blob.CompressionFormat): Promise<Blob> {
		return await syscall.decompress(this, format);
	}

	async extract(format: Blob.ArchiveFormat): Promise<Artifact> {
		return await syscall.extract(this, format);
	}
}

export namespace Branch {
	export type Arg = undefined | Branch | ArgObject | Array<Arg>;

	export type ArgObject = {
		children?: Array<Child>;
	};

	export type Child = { blob: Blob; size: number };

	export type Id = string;

	export type Object_ = { children: Array<Child> };

	export type State = Object_.State<Branch.Id, Branch.Object_>;
}
