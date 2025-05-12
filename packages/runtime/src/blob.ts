import * as tg from "./index.ts";

export let blob = async (...args: tg.Args<Blob.Arg>): Promise<Blob> => {
	return await Blob.new(...args);
};

export class Blob {
	#state: Blob.State;

	constructor(state: Blob.State) {
		this.#state = state;
	}

	get state(): Blob.State {
		return this.#state;
	}

	static withId(id: Blob.Id): Blob {
		return new Blob({ id });
	}

	static async new(...args: tg.Args<Blob.Arg>): Promise<Blob> {
		let arg = await Blob.arg(...args);
		let blob: Blob;
		if (!arg.children || arg.children.length === 0) {
			blob = new Blob({
				object: { bytes: new Uint8Array() },
			});
		} else if (arg.children.length === 1) {
			blob = arg.children[0]!.blob;
		} else {
			blob = new Blob({
				object: { children: arg.children },
			});
		}
		return blob;
	}

	static async leaf(
		...args: tg.Args<undefined | string | Uint8Array | tg.Blob>
	): Promise<Blob> {
		let resolved = await Promise.all(args.map(tg.resolve));
		let objects = await Promise.all(
			resolved.map(async (arg) => {
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
		let length = objects.reduce(
			(length, bytes) => length + bytes.byteLength,
			0,
		);
		let bytes = new Uint8Array(length);
		let offset = 0;
		for (let entry of objects) {
			bytes.set(entry, offset);
			offset += entry.byteLength;
		}
		let object = { bytes };
		return new Blob({ object });
	}

	static async branch(...args: tg.Args<Blob.Arg>): Promise<Blob> {
		let arg = await Blob.arg(...args);
		return new Blob({
			object: { children: arg.children ?? [] },
		});
	}

	static async arg(...args: tg.Args<Blob.Arg>): Promise<Blob.ArgObject> {
		return await tg.Args.apply({
			args,
			map: async (arg) => {
				if (arg === undefined) {
					return { children: [] };
				} else if (typeof arg === "string") {
					let bytes = tg.encoding.utf8.encode(arg);
					let blob = new Blob({
						object: { bytes },
					});
					let length = bytes.length;
					return { children: [{ blob, length }] };
				} else if (arg instanceof Uint8Array) {
					let bytes = arg;
					let blob = new Blob({
						object: { bytes },
					});
					let length = bytes.length;
					return { children: [{ blob, length }] };
				} else if (arg instanceof Blob) {
					let length = await arg.length();
					let child = { blob: arg, length };
					return {
						children: [child],
					};
				} else {
					return arg;
				}
			},
			reduce: {
				children: "append",
			},
		});
	}

	static expect(value: unknown): Blob {
		tg.assert(value instanceof Blob);
		return value;
	}

	static assert(value: unknown): asserts value is Blob {
		tg.assert(value instanceof Blob);
	}

	async id(): Promise<Blob.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<Blob.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("object_load", this.#state.id!);
			tg.assert(object.kind === "blob");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("object_store", {
				kind: "blob",
				value: this.#state.object!,
			});
		}
	}

	async children(): Promise<Array<Blob.Child>> {
		let object = await this.object();
		if ("children" in object) {
			return object.children;
		} else {
			return [];
		}
	}

	async length(): Promise<number> {
		let object = await this.object();
		if ("children" in object) {
			return object.children
				.map(({ length }) => length)
				.reduce((a, b) => a + b, 0);
		} else {
			return object.bytes.byteLength;
		}
	}

	async bytes(): Promise<Uint8Array> {
		return await syscall("blob_read", this);
	}

	async text(): Promise<string> {
		return tg.encoding.utf8.decode(await this.bytes());
	}
}

export namespace Blob {
	export type Arg = undefined | string | Uint8Array | tg.Blob | ArgObject;

	export type ArgObject = {
		children?: Array<Child> | undefined;
	};

	export type Id = string;

	export type Object = Leaf | Branch;

	export type Leaf = {
		bytes: Uint8Array;
	};

	export type Branch = {
		children: Array<Child>;
	};

	export type Child = {
		blob: Blob;
		length: number;
	};

	export type State = tg.Object.State<tg.Blob.Id, tg.Blob.Object>;
}
