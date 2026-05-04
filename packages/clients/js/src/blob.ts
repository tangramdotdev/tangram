import * as tg from "./index.ts";
import { unindent } from "./template.ts";

/** Create a blob. */
export function blob(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<string>
): tg.Blob.Builder;
export function blob(...args: tg.Args<tg.Blob.Arg>): tg.Blob.Builder;
export function blob(
	firstArg:
		| TemplateStringsArray
		| tg.Unresolved<tg.ValueOrMaybeMutationMap<tg.Blob.Arg>>,
	...args: tg.Args<tg.Blob.Arg>
): tg.Blob.Builder {
	return new tg.Blob.Builder(firstArg, ...args);
}

export class Blob {
	#state: tg.Object.State;

	constructor(arg: {
		id?: tg.Blob.Id;
		object?: tg.Blob.Object;
		stored: boolean;
	}) {
		let object =
			arg.object !== undefined
				? { kind: "blob" as const, value: arg.object }
				: undefined;
		this.#state = new tg.Object.State({
			id: arg.id,
			object,
			stored: arg.stored,
		});
	}

	get state(): tg.Object.State {
		return this.#state;
	}

	/** Get a blob with an ID. */
	static withId(id: tg.Blob.Id): tg.Blob {
		return new tg.Blob({ id, stored: true });
	}

	static withObject(object: tg.Blob.Object): tg.Blob {
		return new tg.Blob({ object, stored: false });
	}

	static fromData(data: tg.Blob.Data): tg.Blob {
		return tg.Blob.withObject(tg.Blob.Object.fromData(data));
	}

	/** Create a blob. */
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

	static leaf(
		...args: tg.Args<undefined | string | Uint8Array | tg.Blob>
	): tg.Blob.Builder {
		return tg.Blob.Builder.leaf(...args);
	}

	static branch(...args: tg.Args<tg.Blob.Arg>): tg.Blob.Builder {
		return tg.Blob.Builder.branch(...args);
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
					let length = await arg.length;
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

	/** Expect that a value is a `tg.Blob`. */
	static expect(value: unknown): tg.Blob {
		tg.assert(value instanceof tg.Blob);
		return value;
	}

	/** Assert that a value is a `tg.Blob`. */
	static assert(value: unknown): asserts value is tg.Blob {
		tg.assert(value instanceof tg.Blob);
	}

	/** Get this blob's ID. */
	get id(): tg.Blob.Id {
		let id = this.#state.id;
		tg.assert(tg.Object.Id.kind(id) === "blob");
		return id;
	}

	async object(): Promise<tg.Blob.Object> {
		let object = await this.#state.load();
		tg.assert(object.kind === "blob");
		return object.value;
	}

	async load(): Promise<tg.Blob.Object> {
		let object = await this.#state.load();
		tg.assert(object.kind === "blob");
		return object.value;
	}

	unload(): void {
		this.#state.unload();
	}

	/** Store this blob. */
	async store(): Promise<tg.Blob.Id> {
		await tg.Value.store(this);
		return this.id;
	}

	get children(): Promise<Array<tg.Object>> {
		return this.#state.children;
	}

	/** Get this blob's length. */
	get length(): Promise<number> {
		return (async () => {
			let object = await this.object();
			if ("children" in object) {
				return object.children
					.map(({ length }) => length)
					.reduce((a, b) => a + b, 0);
			} else {
				return object.bytes.byteLength;
			}
		})();
	}

	/** Read from this blob. */
	async read(options?: tg.Blob.ReadOptions): Promise<Uint8Array> {
		let id = await this.store();
		const arg = { blob: id, ...options };
		return await tg.handle.read(arg);
	}

	/** Read this entire blob to a `Uint8Array`. */
	get bytes(): Promise<Uint8Array> {
		return this.read();
	}

	/** Read this entire blob to a string. */
	get text(): Promise<string> {
		return (async () => {
			return tg.encoding.utf8.decode(await this.bytes);
		})();
	}
}

export namespace Blob {
	export type Id = string;

	export class Builder {
		#args: Array<any>;
		#create: (...args: Array<any>) => Promise<tg.Blob>;

		constructor(
			raw: boolean,
			strings: TemplateStringsArray,
			...placeholders: tg.Args<string>
		);
		constructor(
			strings: TemplateStringsArray,
			...placeholders: tg.Args<string>
		);
		constructor(
			firstArg:
				| TemplateStringsArray
				| tg.Unresolved<tg.ValueOrMaybeMutationMap<tg.Blob.Arg>>,
			...args: tg.Args<tg.Blob.Arg>
		);
		constructor(...args: tg.Args<tg.Blob.Arg>);
		constructor(...args: any[]) {
			let raw = false;
			if (typeof args[0] === "boolean") {
				raw = args[0];
				args = args.slice(1);
			}
			let firstArg = args[0];
			if (Array.isArray(firstArg) && "raw" in firstArg) {
				let strings = firstArg as TemplateStringsArray;
				let placeholders = args.slice(1) as tg.Args<string>;
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
				this.#args = [string];
			} else {
				this.#args = args;
			}
			this.#create = tg.Blob.new;
		}

		static leaf(
			...args: tg.Args<undefined | string | Uint8Array | tg.Blob>
		): tg.Blob.Builder {
			let builder = new tg.Blob.Builder();
			builder.#args = args;
			builder.#create = tg.Blob.Builder.createLeaf;
			return builder;
		}

		static async createLeaf(
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
						return await arg.bytes;
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

		static branch(...args: tg.Args<tg.Blob.Arg>): tg.Blob.Builder {
			let builder = new tg.Blob.Builder();
			builder.#args = args;
			builder.#create = tg.Blob.Builder.createBranch;
			return builder;
		}

		static async createBranch(...args: tg.Args<tg.Blob.Arg>): Promise<tg.Blob> {
			let arg = await tg.Blob.arg(...args);
			return tg.Blob.withObject({ children: arg.children ?? [] });
		}

		then<TResult1 = tg.Blob, TResult2 = never>(
			onfulfilled?:
				| ((value: tg.Blob) => TResult1 | PromiseLike<TResult1>)
				| undefined
				| null,
			onrejected?:
				| ((reason: any) => TResult2 | PromiseLike<TResult2>)
				| undefined
				| null,
		): PromiseLike<TResult1 | TResult2> {
			return this.#create(...this.#args).then(onfulfilled, onrejected);
		}
	}

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

		export let children = (data: tg.Blob.Data): Array<tg.Object.Id> => {
			if ("children" in data) {
				return data.children.map((child) => child.blob);
			} else {
				return [];
			}
		};
	}

	export type ReadOptions = {
		position?: number | string | undefined;
		length?: number | undefined;
		size?: number | undefined;
	};

	export let raw = (
		strings: TemplateStringsArray,
		...placeholders: tg.Args<string>
	): tg.Blob.Builder => {
		return new tg.Blob.Builder(true, strings, ...placeholders);
	};
}
