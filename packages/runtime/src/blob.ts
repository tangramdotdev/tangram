import * as tg from "./index.ts";
import { unindent } from "./template.ts";

export async function blob(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<string>
): Promise<tg.Blob>;
export async function blob(...args: tg.Args<tg.Blob.Arg>): Promise<tg.Blob>;
export async function blob(
	firstArg:
		| TemplateStringsArray
		| tg.Unresolved<tg.ValueOrMaybeMutationMap<tg.Blob.Arg>>,
	...args: tg.Args<tg.Blob.Arg>
): Promise<tg.Blob> {
	return await inner(false, firstArg, ...args);
}

async function inner(
	raw: boolean,
	firstArg:
		| TemplateStringsArray
		| tg.Unresolved<tg.ValueOrMaybeMutationMap<tg.Blob.Arg>>,
	...args: tg.Args<tg.Blob.Arg>
): Promise<tg.Blob> {
	if (Array.isArray(firstArg) && "raw" in firstArg) {
		let strings = firstArg;
		let placeholders = args as tg.Args<string>;
		let components = [];
		for (let i = 0; i < strings.length - 1; i++) {
			let string = strings[i]!;
			components.push(string);
			let placeholder = placeholders[i]!;
			components.push(placeholder);
		}
		components.push(strings[strings.length - 1]!);
		let string = components.join("");
		if (!raw) {
			string = unindent([string]).join("");
		}
		return await tg.Blob.new(string);
	} else {
		return await tg.Blob.new(firstArg as tg.Blob.Arg, ...args);
	}
}

export class Blob {
	#state: tg.Blob.State;

	constructor(state: tg.Blob.State) {
		this.#state = state;
	}

	get state(): tg.Blob.State {
		return this.#state;
	}

	static withId(id: tg.Blob.Id): tg.Blob {
		return new tg.Blob({ id, stored: true });
	}

	static withObject(object: tg.Blob.Object): tg.Blob {
		return new tg.Blob({ object, stored: false });
	}

	static fromData(data: tg.Blob.Data): tg.Blob {
		return tg.Blob.withObject(Blob.Object.fromData(data));
	}

	static async new(...args: tg.Args<tg.Blob.Arg>): Promise<tg.Blob> {
		let arg = await tg.Blob.arg(...args);
		let blob: tg.Blob;
		if (!arg.children || arg.children.length === 0) {
			blob = tg.Blob.withObject({ bytes: new Uint8Array() });
		} else if (arg.children.length === 1) {
			blob = arg.children[0]!.blob;
		} else {
			blob = tg.Blob.withObject({ children: arg.children });
		}
		return blob;
	}

	static async leaf(
		...args: tg.Args<undefined | string | Uint8Array | tg.Blob>
	): Promise<tg.Blob> {
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
		return tg.Blob.withObject(object);
	}

	static async branch(...args: tg.Args<tg.Blob.Arg>): Promise<tg.Blob> {
		let arg = await tg.Blob.arg(...args);
		return tg.Blob.withObject({ children: arg.children ?? [] });
	}

	static async arg(...args: tg.Args<tg.Blob.Arg>): Promise<tg.Blob.Arg.Object> {
		return await tg.Args.apply({
			args,
			map: async (arg) => {
				if (arg === undefined) {
					return { children: [] };
				} else if (typeof arg === "string") {
					let bytes = tg.encoding.utf8.encode(arg);
					let blob = tg.Blob.withObject({ bytes });
					let length = bytes.length;
					return { children: [{ blob, length }] };
				} else if (arg instanceof Uint8Array) {
					let bytes = arg;
					let blob = tg.Blob.withObject({ bytes });
					let length = bytes.length;
					return { children: [{ blob, length }] };
				} else if (arg instanceof tg.Blob) {
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

	static expect(value: unknown): tg.Blob {
		tg.assert(value instanceof tg.Blob);
		return value;
	}

	static assert(value: unknown): asserts value is tg.Blob {
		tg.assert(value instanceof tg.Blob);
	}

	get id(): tg.Blob.Id {
		if (this.#state.id! !== undefined) {
			return this.#state.id;
		}
		let object = this.#state.object!;
		let data = tg.Blob.Object.toData(object);
		let id = syscall("object_id", { kind: "blob", value: data });
		this.#state.id = id;
		return id;
	}

	async object(): Promise<tg.Blob.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load(): Promise<tg.Blob.Object> {
		if (this.#state.object === undefined) {
			let data = await syscall("object_get", this.#state.id!);
			tg.assert(data.kind === "blob");
			let object = tg.Blob.Object.fromData(data.value);
			this.#state.object = object;
		}
		return this.#state.object!;
	}

	async store(): Promise<tg.Blob.Id> {
		await tg.Value.store(this);
		return this.id;
	}

	async children(): Promise<Array<tg.Object>> {
		let object = await this.object();
		return tg.Blob.Object.children(object);
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

	async read(arg?: tg.Blob.ReadArg): Promise<Uint8Array> {
		let id = await this.store();
		return await syscall("blob_read", id, arg ?? {});
	}

	async bytes(): Promise<Uint8Array> {
		return await this.read();
	}

	async text(): Promise<string> {
		return tg.encoding.utf8.decode(await this.bytes());
	}
}

export namespace Blob {
	export type Id = string;

	export type State = tg.Object.State<tg.Blob.Id, tg.Blob.Object>;

	export type Arg =
		| undefined
		| string
		| Uint8Array
		| tg.Blob
		| tg.Blob.Arg.Object;

	export namespace Arg {
		export type Object = {
			children?: Array<tg.Blob.Child> | undefined;
		};
	}

	export type Object = tg.Blob.Leaf | tg.Blob.Branch;

	export namespace Object {
		export let toData = (object: tg.Blob.Object): tg.Blob.Data => {
			if ("bytes" in object) {
				return {
					bytes: tg.encoding.base64.encode(object.bytes),
				};
			} else {
				return {
					children: object.children.map((child) => ({
						blob: child.blob.id,
						length: child.length,
					})),
				};
			}
		};

		export let fromData = (data: tg.Blob.Data): tg.Blob.Object => {
			if ("bytes" in data) {
				return {
					bytes: tg.encoding.base64.decode(data.bytes),
				};
			} else {
				return {
					children: data.children.map((child) => ({
						blob: tg.Blob.withId(child.blob),
						length: child.length,
					})),
				};
			}
		};

		export let children = (object: tg.Blob.Object): Array<tg.Object> => {
			if ("children" in object) {
				return object.children.map(({ blob }) => blob);
			} else {
				return [];
			}
		};
	}

	export type Leaf = {
		bytes: Uint8Array;
	};

	export type Branch = {
		children: Array<tg.Blob.Child>;
	};

	export type Child = {
		blob: tg.Blob;
		length: number;
	};

	export type Data = tg.Blob.Data.Leaf | tg.Blob.Data.Branch;

	export namespace Data {
		export type Leaf = {
			bytes: string;
		};

		export type Branch = {
			children: Array<tg.Blob.Data.Child>;
		};

		export type Child = {
			blob: tg.Blob.Id;
			length: number;
		};
	}

	export type ReadArg = {
		position?: number | string | undefined;
		length?: number | undefined;
	};

	export let raw = async (
		strings: TemplateStringsArray,
		...placeholders: tg.Args<string>
	): Promise<tg.Blob> => {
		return await inner(true, strings, ...placeholders);
	};
}
