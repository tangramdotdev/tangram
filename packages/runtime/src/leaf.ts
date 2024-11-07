import * as tg from "./index.ts";
import { flatten } from "./util.ts";

export let leaf = async (...args: tg.Args<Leaf.Arg>): Promise<Leaf> => {
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

	static async new(...args: tg.Args<Leaf.Arg>): Promise<Leaf> {
		let resolved = await Promise.all(args.map(tg.resolve));
		let flattened = flatten(resolved);
		let objects = await Promise.all(
			flattened.map(async (arg) => {
				if (arg === undefined) {
					return new Uint8Array();
				} else if (typeof arg === "string") {
					return tg.encoding.utf8.encode(arg);
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
		tg.assert(value instanceof Leaf);
		return value;
	}

	static assert(value: unknown): asserts value is Leaf {
		tg.assert(value instanceof Leaf);
	}

	async id(): Promise<Leaf.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<Leaf.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("object_load", this.#state.id!);
			tg.assert(object.kind === "leaf");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("object_store", {
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
		return tg.encoding.utf8.decode(await syscall("blob_read", this));
	}
}

export namespace Leaf {
	export type Arg = undefined | string | Uint8Array | tg.Leaf;

	export type Id = string;

	export type Object = { bytes: Uint8Array };

	export type State = tg.Object.State<tg.Leaf.Id, tg.Leaf.Object>;
}
