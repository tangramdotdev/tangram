import type { Args } from "./args.ts";
import { assert as assert_ } from "./assert.ts";
import * as encoding from "./encoding.ts";
import type { Object_ } from "./object.ts";
import { resolve } from "./resolve.ts";
import { flatten } from "./util.ts";

export let leaf = async (...args: Args<Leaf.Arg>): Promise<Leaf> => {
	return await Leaf.new(...args);
};

export class Leaf {
	#state: Leaf.State;

	constructor(state: Leaf.State) {
		this.#state = state;
	}

	get state(): Leaf.State {
		return this.#state;
	}

	static withId(id: Leaf.Id): Leaf {
		return new Leaf({ id });
	}

	static async new(...args: Args<Leaf.Arg>): Promise<Leaf> {
		let resolved = await Promise.all(args.map(resolve));
		let flattened = flatten(resolved);
		let objects = await Promise.all(
			flattened.map(async (arg) => {
				if (arg === undefined) {
					return new Uint8Array();
				} else if (typeof arg === "string") {
					return encoding.utf8.encode(arg);
				} else if (arg instanceof Uint8Array) {
					return arg;
				} else {
					return await arg.bytes();
				}
			}),
		);
		let size = objects.reduce((size, bytes) => size + bytes.byteLength, 0);
		let bytes = new Uint8Array(size);
		let offset = 0;
		for (let entry of objects) {
			bytes.set(entry, offset);
			offset += entry.byteLength;
		}
		let object = { bytes };
		return new Leaf({ object: object });
	}

	static expect(value: unknown): Leaf {
		assert_(value instanceof Leaf);
		return value;
	}

	static assert(value: unknown): asserts value is Leaf {
		assert_(value instanceof Leaf);
	}

	async id(): Promise<Leaf.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<Leaf.Object_> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("load", this.#state.id!);
			assert_(object.kind === "leaf");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("store", {
				kind: "leaf",
				value: this.#state.object!,
			});
		}
	}

	async size(): Promise<number> {
		return (await this.object()).bytes.byteLength;
	}

	async bytes(): Promise<Uint8Array> {
		return (await this.object()).bytes;
	}

	async text(): Promise<string> {
		return encoding.utf8.decode(await syscall("read", this));
	}
}

export namespace Leaf {
	export type Arg = undefined | string | Uint8Array | Leaf;

	export type Id = string;

	export type Object_ = { bytes: Uint8Array };

	export type State = Object_.State<Leaf.Id, Leaf.Object_>;
}
