import { Args } from "./args.ts";
import { Artifact } from "./artifact.ts";
import { assert as assert_, unreachable } from "./assert.ts";
import { Blob } from "./blob.ts";
import * as encoding from "./encoding.ts";
import { Mutation, mutation } from "./mutation.ts";
import { Object_ } from "./object.ts";
import * as syscall from "./syscall.ts";
import { MutationMap } from "./util.ts";

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
		type Apply = {
			bytes: Array<Uint8Array>;
		};
		let { bytes: bytes_ } = await Args.apply<Leaf.Arg, Apply>(
			args,
			async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (typeof arg === "string") {
					return {
						bytes: await mutation({
							kind: "array_append",
							values: [encoding.utf8.encode(arg)],
						}),
					};
				} else if (arg instanceof Uint8Array) {
					return {
						bytes: await mutation({
							kind: "array_append",
							values: [arg],
						}),
					};
				} else if (Leaf.is(arg)) {
					return {
						bytes: await mutation({
							kind: "array_append",
							values: [await arg.bytes()],
						}),
					};
				} else if (typeof arg === "object") {
					let object: MutationMap<Apply> = {};
					if (arg.bytes !== undefined) {
						object.bytes = Mutation.is(arg.bytes)
							? arg.bytes
							: await mutation({
									kind: "array_append",
									values: [arg.bytes],
								});
					}
					return object;
				} else {
					return unreachable();
				}
			},
		);
		bytes_ ??= [];
		let size = bytes_.reduce((size, bytes) => size + bytes.byteLength, 0);
		let bytes = new Uint8Array(size);
		let offset = 0;
		for (let entry of bytes_) {
			bytes.set(entry, offset);
			offset += entry.byteLength;
		}
		return new Leaf({ object: { bytes } });
	}

	static is(value: unknown): value is Leaf {
		return value instanceof Leaf;
	}

	static expect(value: unknown): Leaf {
		assert_(Leaf.is(value));
		return value;
	}

	static assert(value: unknown): asserts value is Leaf {
		assert_(Leaf.is(value));
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
			let object = await syscall.load(this.#state.id!);
			assert_(object.kind === "leaf");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall.store({
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

export namespace Leaf {
	export type Arg =
		| undefined
		| string
		| Uint8Array
		| Leaf
		| ArgObject
		| Array<Arg>;

	export type ArgObject = {
		bytes?: Uint8Array;
	};

	export type Id = string;

	export type Object_ = { bytes: Uint8Array };

	export type State = Object_.State<Leaf.Id, Leaf.Object_>;
}
