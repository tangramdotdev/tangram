import * as tg from "./index.ts";
import { unindent } from "./template.ts";

/** Create a file. */
export function file(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<string>
): tg.File.Builder;
export function file(...args: tg.Args<tg.File.Arg>): tg.File.Builder;
export function file(
	firstArg?:
		| TemplateStringsArray
		| tg.Unresolved<tg.ValueOrMaybeMutationMap<tg.File.Arg>>,
	...args: tg.Args<tg.File.Arg>
): tg.File.Builder {
	return new tg.File.Builder(firstArg, ...args);
}

/** A file. */
export class File {
	#state: tg.Object.State;

	constructor(arg: {
		id?: tg.File.Id;
		object?: tg.File.Object;
		stored: boolean;
	}) {
		let object =
			arg.object !== undefined
				? { kind: "file" as const, value: arg.object }
				: undefined;
		this.#state = new tg.Object.State({
			...(arg.id !== undefined ? { id: arg.id } : {}),
			...(object !== undefined ? { object } : {}),
			stored: arg.stored,
		});
	}

	get state(): tg.Object.State {
		return this.#state;
	}

	/** Get a file with an ID. */
	static withId(id: tg.File.Id): tg.File {
		return new tg.File({ id, stored: true });
	}

	static withPointer(pointer: tg.Graph.Pointer): tg.File {
		return new tg.File({ object: pointer, stored: false });
	}

	static withObject(object: tg.File.Object): tg.File {
		return new tg.File({ object, stored: false });
	}

	static fromData(data: tg.File.Data): tg.File {
		return tg.File.withObject(tg.File.Object.fromData(data));
	}

	/** Create a file. */
	static async new(...args: tg.Args<tg.File.Arg>): Promise<tg.File> {
		let arg: Exclude<tg.File.Arg.Object, tg.Graph.Arg.Pointer>;
		if (args.length === 1) {
			let resolved = await tg.resolve(args[0]!);
			if (tg.Graph.Arg.Pointer.is(resolved)) {
				return tg.File.withObject(tg.Graph.Pointer.fromArg(resolved));
			}
			arg = await tg.File.argResolved(resolved);
		} else {
			arg = await tg.File.arg(...args);
		}
		let contents =
			arg.contents != null ? await tg.blob(arg.contents) : await tg.blob();
		let dependencies = Object.fromEntries(
			Object.entries(arg.dependencies ?? {}).map(([reference, value]) => {
				if (value === null) {
					return [reference, null];
				}
				let dependency: tg.Graph.Dependency;
				if (
					typeof value === "number" ||
					"index" in value ||
					tg.Object.is(value)
				) {
					let item = tg.Graph.Edge.fromArg(value);
					dependency = { item, options: {} };
				} else if (value.item === null) {
					dependency = { item: null, options: value.options ?? {} };
				} else {
					let item = tg.Graph.Edge.fromArg(value.item);
					dependency = { item, options: value.options ?? {} };
				}
				return [reference, dependency];
			}),
		);
		let executable = arg.executable ?? false;
		let object: tg.Graph.File = { contents, dependencies, executable };
		if (arg.module != null) {
			object.module = arg.module;
		}
		return tg.File.withObject(object);
	}

	static async arg(
		...args: tg.Args<tg.File.Arg>
	): Promise<Exclude<tg.File.Arg.Object, tg.Graph.Arg.Pointer>> {
		type Arg = Exclude<tg.File.Arg.Object, tg.Graph.Arg.Pointer>;
		return await tg.File.argResolved(
			...(await Promise.all(args.map(tg.resolve))),
		);
	}

	static async argResolved(
		...args: Array<tg.ValueOrMaybeMutationMap<tg.File.Arg>>
	): Promise<Exclude<tg.File.Arg.Object, tg.Graph.Arg.Pointer>> {
		type Arg = Exclude<tg.File.Arg.Object, tg.Graph.Arg.Pointer>;
		return await tg.Args.applyResolved<tg.File.Arg, Arg>({
			args,
			map: async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (
					typeof arg === "string" ||
					arg instanceof Uint8Array ||
					arg instanceof tg.Blob
				) {
					return { contents: arg };
				} else if (arg instanceof tg.File) {
					return {
						contents: await arg.contents,
						dependencies: await arg.dependencies,
					};
				} else {
					return arg as Arg;
				}
			},
			reduce: {
				contents: (a, b) =>
					tg.blob(...(a != null ? [a] : []), ...(b != null ? [b] : [])),
				dependencies: "merge",
			},
		});
	}

	/** Expect that a value is a `tg.File`. */
	static expect(value: unknown): tg.File {
		tg.assert(value instanceof tg.File);
		return value;
	}

	/** Assert that a value is a `tg.File`. */
	static assert(value: unknown): asserts value is tg.File {
		tg.assert(value instanceof tg.File);
	}

	/** Get this file's ID. */
	get id(): tg.File.Id {
		let id = this.#state.id;
		tg.assert(tg.Object.Id.kind(id) === "file");
		return id;
	}

	async object(): Promise<tg.File.Object> {
		let object = await this.#state.load();
		tg.assert(object.kind === "file");
		return object.value;
	}

	async load(): Promise<tg.File.Object> {
		let object = await this.#state.load();
		tg.assert(object.kind === "file");
		return object.value;
	}

	unload(): void {
		this.#state.unload();
	}

	/** Store this file. */
	async store(): Promise<tg.File.Id> {
		await tg.Value.store(this);
		return this.id;
	}

	get children(): Promise<Array<tg.Object>> {
		return this.#state.children;
	}

	/** Get this file's contents. */
	get contents(): Promise<tg.Blob> {
		return (async () => {
			let object = await this.object();
			let contents: tg.Blob;
			if ("index" in object) {
				let graph = object.graph;
				tg.assert(graph != null);
				let nodes = await graph.nodes;
				let node = nodes[object.index];
				tg.assert(node !== undefined);
				tg.assert(node.kind === "file");
				contents = node.contents;
			} else {
				tg.assert(object.contents);
				contents = object.contents;
			}

			tg.Object.inheritToken(contents, this.#state.token);

			return contents;
		})();
	}

	/** Get this file's dependencies. */
	get dependencies(): Promise<{
		[reference: tg.Reference.String]: tg.Referent<tg.Object | null> | null;
	}> {
		return (async () => {
			let object = await this.object();
			if ("index" in object) {
				let graph = object.graph;
				tg.assert(graph != null);
				let nodes = await graph.nodes;
				let node = nodes[object.index];
				tg.assert(node !== undefined);
				tg.assert(node.kind === "file");
				let dependencies = node.dependencies;
				return Object.fromEntries(
					await Promise.all(
						Object.entries(dependencies).map(
							async ([reference, dependency]) => {
								if (dependency === null) {
									return [reference, null];
								}
								let object: tg.Object | null;
								if (dependency.item === null) {
									object = null;
								} else if (typeof dependency.item === "number") {
									object = await graph.get(dependency.item);
								} else if ("index" in dependency.item) {
									object = await (dependency.item.graph ?? graph).get(
										dependency.item.index,
									);
								} else {
									object = dependency.item;
								}
								if (object !== null) {
									tg.Object.inheritToken(object, this.#state.token);
								}
								let value: tg.Referent<tg.Object | null> = {
									item: object,
									options: dependency.options ?? {},
								};
								return [reference, value];
							},
						),
					),
				);
			} else {
				let dependencies = object.dependencies;
				return Object.fromEntries(
					await Promise.all(
						Object.entries(dependencies).map(
							async ([reference, dependency]) => {
								if (dependency === null) {
									return [reference, null];
								}
								let object: tg.Object | null;
								if (dependency.item === null) {
									object = null;
								} else {
									tg.assert(typeof dependency.item === "object");
									if ("index" in dependency.item) {
										tg.assert(dependency.item.graph != null);
										object = await dependency.item.graph.get(
											dependency.item.index,
										);
									} else {
										object = dependency.item;
									}
								}
								if (object !== null) {
									tg.Object.inheritToken(object, this.#state.token);
								}
								let value: tg.Referent<tg.Object | null> = {
									item: object,
									options: dependency.options ?? {},
								};
								return [reference, value];
							},
						),
					),
				);
			}
		})();
	}

	/** Get this file's dependencies as an array. */
	get dependencyObjects(): Promise<Array<tg.Object>> {
		return (async () => {
			let dependencies = await this.dependencies;
			if (dependencies === undefined) {
				return [];
			} else {
				let items = [];
				for (let dependency of Object.values(dependencies)) {
					if (dependency !== null && dependency.item !== null) {
						items.push(dependency.item);
					}
				}
				return items;
			}
		})();
	}

	/** Get this file's executable bit. */
	get executable(): Promise<boolean> {
		return (async () => {
			let object = await this.object();
			if ("index" in object) {
				let graph = object.graph;
				tg.assert(graph != null);
				let nodes = await graph.nodes;
				let node = nodes[object.index];
				tg.assert(node !== undefined);
				tg.assert(node.kind === "file");
				return node.executable;
			} else {
				return object.executable;
			}
		})();
	}

	/** Get this file's module kind. */
	get module(): Promise<string | null> {
		return (async () => {
			let object = await this.object();
			if ("index" in object) {
				let graph = object.graph;
				tg.assert(graph != null);
				let nodes = await graph.nodes;
				let node = nodes[object.index];
				tg.assert(node !== undefined);
				tg.assert(node.kind === "file");
				return node.module ?? null;
			} else {
				return object.module ?? null;
			}
		})();
	}

	/** Get the length of this file's contents. */
	get length(): Promise<number> {
		return (async () => {
			return (await this.contents).length;
		})();
	}

	/** Read from this file. */
	async read(options?: tg.Blob.ReadOptions | null): Promise<Uint8Array> {
		return (await this.contents).read(options);
	}

	/** Get this file's contents as a `Uint8Array`. */
	get bytes(): Promise<Uint8Array> {
		return (async () => {
			return (await this.contents).bytes;
		})();
	}

	/** Get this file's contents as a string. This method throws an error if the contents are not valid UTF-8. */
	get text(): Promise<string> {
		return (async () => {
			return (await this.contents).text;
		})();
	}
}

export namespace File {
	export type Id = string;

	export class Builder {
		#args: tg.Args<tg.File.Arg>;

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
			firstArg?:
				| TemplateStringsArray
				| tg.Unresolved<tg.ValueOrMaybeMutationMap<tg.File.Arg>>,
			...args: tg.Args<tg.File.Arg>
		);
		constructor(...args: tg.Args<tg.File.Arg>);
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
		}

		contents(
			contents: tg.Unresolved<tg.MaybeMutation<tg.Blob.Arg> | null>,
		): this {
			this.#args.push({ contents });
			return this;
		}

		dependencies(
			dependencies: tg.Unresolved<tg.MaybeMutation<
				Exclude<tg.Graph.Arg.File["dependencies"], undefined>
			> | null>,
		): this {
			this.#args.push({ dependencies });
			return this;
		}

		dependency(
			reference: tg.Reference.String,
			value: tg.Unresolved<tg.Graph.Arg.Dependency | null>,
		): this {
			let dependencies: tg.Unresolved<
				Exclude<tg.Graph.Arg.File["dependencies"], undefined>
			> = {
				[reference]: value,
			};
			this.#args.push({ dependencies });
			return this;
		}

		executable(
			executable: tg.Unresolved<tg.MaybeMutation<boolean> | null> = true,
		): this {
			this.#args.push({ executable });
			return this;
		}

		module(module: tg.Unresolved<tg.MaybeMutation<string> | null>): this {
			this.#args.push({ module });
			return this;
		}

		then<TResult1 = tg.File, TResult2 = never>(
			onfulfilled?:
				| ((value: tg.File) => TResult1 | PromiseLike<TResult1>)
				| undefined
				| null,
			onrejected?:
				| ((reason: any) => TResult2 | PromiseLike<TResult2>)
				| undefined
				| null,
		): PromiseLike<TResult1 | TResult2> {
			return tg.File.new(...this.#args).then(onfulfilled, onrejected);
		}
	}

	export type Arg =
		| string
		| Uint8Array
		| tg.Blob
		| tg.File
		| tg.File.Arg.Object;

	export namespace Arg {
		export type Object = tg.Graph.Arg.Pointer | tg.Graph.Arg.File;
	}

	export type Object = tg.Graph.Pointer | tg.Graph.File;

	export namespace Object {
		export let toData = (object: tg.File.Object): tg.File.Data => {
			if ("index" in object) {
				return tg.Graph.Pointer.toData(object);
			} else {
				return tg.Graph.File.toData(object);
			}
		};

		export let fromData = (data: tg.File.Data): tg.File.Object => {
			if (tg.Graph.Data.Pointer.is(data)) {
				return tg.Graph.Pointer.fromData(data);
			} else {
				return tg.Graph.File.fromData(data);
			}
		};

		export let children = (object: tg.File.Object): Array<tg.Object> => {
			if ("index" in object) {
				return tg.Graph.Pointer.children(object);
			} else {
				return tg.Graph.File.children(object);
			}
		};
	}

	export type Data = tg.Graph.Data.Pointer | tg.Graph.Data.File;

	export namespace Data {
		export let children = (data: tg.File.Data): Array<tg.Object.Id> => {
			if (tg.Graph.Data.Pointer.is(data)) {
				return tg.Graph.Data.Pointer.children(data);
			} else {
				return tg.Graph.Data.File.children(data);
			}
		};
	}

	export let raw = (
		strings: TemplateStringsArray,
		...placeholders: tg.Args<string>
	): tg.File.Builder => {
		return new tg.File.Builder(true, strings, ...placeholders);
	};
}
