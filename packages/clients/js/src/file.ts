import * as tg from "./index.ts";
import { unindent } from "./template.ts";

export async function file(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<string>
): Promise<tg.File>;
export async function file(...args: tg.Args<tg.File.Arg>): Promise<tg.File>;
export async function file(
	firstArg:
		| TemplateStringsArray
		| tg.Unresolved<tg.ValueOrMaybeMutationMap<tg.File.Arg>>,
	...args: tg.Args<tg.File.Arg>
): Promise<tg.File> {
	return await inner(false, firstArg, ...args);
}

async function inner(
	raw: boolean,
	firstArg:
		| TemplateStringsArray
		| tg.Unresolved<tg.ValueOrMaybeMutationMap<tg.File.Arg>>,
	...args: tg.Args<tg.File.Arg>
): Promise<tg.File> {
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
		return await tg.File.new(string);
	} else {
		return await tg.File.new(firstArg as tg.File.Arg, ...args);
	}
}

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
			id: arg.id,
			object,
			stored: arg.stored,
		});
	}

	get state(): tg.Object.State {
		return this.#state;
	}

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

	static async new(...args: tg.Args<tg.File.Arg>): Promise<tg.File> {
		if (args.length === 1) {
			let arg = await tg.resolve(args[0]);
			if (tg.Graph.Arg.Pointer.is(arg)) {
				return tg.File.withObject(tg.Graph.Pointer.fromArg(arg));
			}
		}
		let arg = await tg.File.arg(...args);
		let contents = await tg.blob(arg.contents);
		let dependencies = Object.fromEntries(
			Object.entries(arg.dependencies ?? {}).map(([reference, value]) => {
				let dependency: tg.Graph.Dependency | undefined;
				if (
					typeof value === "number" ||
					(value && "index" in value) ||
					tg.Object.is(value)
				) {
					let item = tg.Graph.Edge.fromArg(value);
					dependency = { item, options: {} };
				} else if (value) {
					let item =
						value.item !== undefined
							? tg.Graph.Edge.fromArg(value.item)
							: undefined;
					dependency = { item, options: value.options };
				}
				return [reference, dependency];
			}),
		);
		let executable = arg.executable ?? false;
		let object = { contents, dependencies, executable };
		return tg.File.withObject(object);
	}

	static async arg(
		...args: tg.Args<tg.File.Arg>
	): Promise<Exclude<tg.File.Arg.Object, tg.Graph.Arg.Pointer>> {
		type Arg = Exclude<tg.File.Arg.Object, tg.Graph.Arg.Pointer>;
		return await tg.Args.apply<tg.File.Arg, Arg>({
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
						contents: await arg.contents(),
						dependencies: await arg.dependencies(),
					};
				} else {
					return arg as Arg;
				}
			},
			reduce: {
				contents: (a, b) => tg.blob(a, b),
				dependencies: "merge",
			},
		});
	}

	static expect(value: unknown): tg.File {
		tg.assert(value instanceof tg.File);
		return value;
	}

	static assert(value: unknown): asserts value is tg.File {
		tg.assert(value instanceof tg.File);
	}

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

	async store(): Promise<tg.File.Id> {
		await tg.Value.store(this);
		return this.id;
	}

	async children(): Promise<Array<tg.Object>> {
		return this.#state.children();
	}

	async contents(): Promise<tg.Blob> {
		let object = await this.object();
		if ("index" in object) {
			let graph = object.graph;
			tg.assert(graph !== undefined);
			let nodes = await graph.nodes();
			let node = nodes[object.index];
			tg.assert(node !== undefined);
			tg.assert(node.kind === "file");
			return node.contents;
		} else {
			tg.assert(object.contents);
			return object.contents;
		}
	}

	async dependencies(): Promise<{
		[reference: tg.Reference]: tg.Referent<tg.Object> | undefined;
	}> {
		let object = await this.object();
		if ("index" in object) {
			let graph = object.graph;
			tg.assert(graph !== undefined);
			let nodes = await graph.nodes();
			let node = nodes[object.index];
			tg.assert(node !== undefined);
			tg.assert(node.kind === "file");
			let dependencies = node.dependencies;
			return Object.fromEntries(
				await Promise.all(
					Object.entries(dependencies).map(async ([reference, dependency]) => {
						if (!dependency) {
							return [reference, undefined];
						}
						let object: tg.Object | undefined;
						if (dependency.item === undefined) {
							object = undefined;
						} else if (typeof dependency.item === "number") {
							object = await graph.get(dependency.item);
						} else if ("index" in dependency.item) {
							object = await (dependency.item.graph ?? graph).get(
								dependency.item.index,
							);
						} else {
							object = dependency.item;
						}
						let value: tg.Referent<tg.Object | undefined> = {
							item: object,
							options: dependency.options,
						};
						return [reference, value];
					}),
				),
			);
		} else {
			let dependencies = object.dependencies;
			return Object.fromEntries(
				await Promise.all(
					Object.entries(dependencies).map(async ([reference, dependency]) => {
						if (!dependency) {
							return [reference, undefined];
						}
						let object: tg.Object | undefined;
						tg.assert(typeof dependency.item === "object");
						if ("index" in dependency.item) {
							tg.assert(dependency.item.graph !== undefined);
							object = await dependency.item.graph.get(dependency.item.index);
						} else {
							object = dependency.item;
						}
						let value = {
							item: object,
							options: dependency.options,
						};
						return [reference, value];
					}),
				),
			);
		}
	}

	async dependencyObjects(): Promise<Array<tg.Object>> {
		let dependencies = await this.dependencies();
		if (dependencies === undefined) {
			return [];
		} else {
			let items = [];
			for (let dependency of Object.values(dependencies)) {
				if (dependency) {
					items.push(dependency.item);
				}
			}
			return items;
		}
	}

	async executable(): Promise<boolean> {
		let object = await this.object();
		if ("index" in object) {
			let graph = object.graph;
			tg.assert(graph !== undefined);
			let nodes = await graph.nodes();
			let node = nodes[object.index];
			tg.assert(node !== undefined);
			tg.assert(node.kind === "file");
			return node.executable;
		} else {
			return object.executable;
		}
	}

	async length(): Promise<number> {
		return (await this.contents()).length();
	}

	async read(options?: tg.Blob.ReadOptions): Promise<Uint8Array> {
		return (await this.contents()).read(options);
	}

	async bytes(): Promise<Uint8Array> {
		return (await this.contents()).bytes();
	}

	async text(): Promise<string> {
		return (await this.contents()).text();
	}
}

export namespace File {
	export type Id = string;

	export type Arg =
		| undefined
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

	export let raw = async (
		strings: TemplateStringsArray,
		...placeholders: tg.Args<string>
	): Promise<tg.File> => {
		return await inner(true, strings, ...placeholders);
	};
}
