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
		return await File.new(string);
	} else {
		return await File.new(firstArg as tg.File.Arg, ...args);
	}
}

export class File {
	#state: File.State;

	constructor(state: File.State) {
		this.#state = state;
	}

	get state(): File.State {
		return this.#state;
	}

	static withId(id: File.Id): File {
		return new File({ id, stored: true });
	}

	static withObject(object: File.Object): File {
		return new File({ object, stored: false });
	}

	static fromData(data: File.Data): File {
		return File.withObject(File.Object.fromData(data));
	}

	static async new(...args: tg.Args<File.Arg>): Promise<File> {
		if (args.length === 1) {
			let arg = await tg.resolve(args[0]);
			if (typeof arg === "object" && "node" in arg) {
				return tg.File.withObject(arg as tg.Graph.Reference);
			}
		}
		let arg = await tg.File.arg(...args);
		let contents = await tg.blob(arg.contents);
		let dependencies = Object.fromEntries(
			Object.entries(arg.dependencies ?? {}).map(([key, value]) => {
				let referent: tg.Referent<tg.Graph.Edge<tg.Object>> | undefined;
				if (
					typeof value === "number" ||
					(value && "node" in value) ||
					tg.Object.is(value)
				) {
					let item = tg.Graph.Edge.fromArg(value);
					referent = { item, options: {} };
				} else if (value) {
					let item = tg.Graph.Edge.fromArg(value.item);
					referent = { item, options: value.options };
				}
				return [key, referent];
			}),
		);
		let executable = arg.executable ?? false;
		let object = { contents, dependencies, executable };
		return tg.File.withObject(object);
	}

	static async arg(
		...args: tg.Args<tg.File.Arg>
	): Promise<Exclude<tg.File.Arg.Object, tg.Graph.Arg.Reference>> {
		type Arg = Exclude<tg.File.Arg.Object, tg.Graph.Arg.Reference>;
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
				} else if (arg instanceof File) {
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
		tg.assert(value instanceof File);
		return value;
	}

	static assert(value: unknown): asserts value is tg.File {
		tg.assert(value instanceof File);
	}

	get id(): tg.File.Id {
		if (this.#state.id! !== undefined) {
			return this.#state.id;
		}
		let object = this.#state.object!;
		let data = File.Object.toData(object);
		let id = syscall("object_id", { kind: "file", value: data });
		this.#state.id = id;
		return id;
	}

	async object(): Promise<File.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load(): Promise<tg.File.Object> {
		if (this.#state.object === undefined) {
			let data = await syscall("object_get", this.#state.id!);
			tg.assert(data.kind === "file");
			let object = File.Object.fromData(data.value);
			this.#state.object = object;
		}
		return this.#state.object!;
	}

	async store(): Promise<tg.File.Id> {
		await tg.Value.store(this);
		return this.id;
	}

	async children(): Promise<Array<tg.Object>> {
		let object = await this.load();
		return tg.File.Object.children(object);
	}

	async contents(): Promise<tg.Blob> {
		let object = await this.object();
		if ("node" in object) {
			let graph = object.graph;
			tg.assert(graph !== undefined);
			let nodes = await graph.nodes();
			let node = nodes[object.node];
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
		if ("node" in object) {
			let graph = object.graph;
			tg.assert(graph !== undefined);
			let nodes = await graph.nodes();
			let node = nodes[object.node];
			tg.assert(node !== undefined);
			tg.assert(node.kind === "file");
			let dependencies = node.dependencies;
			return Object.fromEntries(
				await Promise.all(
					Object.entries(dependencies).map(async ([reference, referent]) => {
						if (!referent) {
							return [reference, undefined];
						}
						let object: tg.Object | undefined;
						if (typeof referent.item === "number") {
							object = await graph.get(referent.item);
						} else if ("node" in referent.item) {
							object = await (referent.item.graph ?? graph).get(
								referent.item.node,
							);
						} else {
							object = referent.item;
						}
						let value = {
							item: object,
							options: referent.options,
						};
						return [reference, value];
					}),
				),
			);
		} else {
			let dependencies = object.dependencies;
			return Object.fromEntries(
				await Promise.all(
					Object.entries(dependencies).map(async ([reference, referent]) => {
						if (!referent) {
							return [reference, undefined];
						}
						let object: tg.Object | undefined;
						tg.assert(typeof referent.item === "object");
						if ("node" in referent.item) {
							tg.assert(referent.item.graph !== undefined);
							object = await referent.item.graph.get(referent.item.node);
						} else {
							object = referent.item;
						}
						let value = {
							item: object,
							options: referent.options,
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
		if ("node" in object) {
			let graph = object.graph;
			tg.assert(graph !== undefined);
			let nodes = await graph.nodes();
			let node = nodes[object.node];
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

	async read(arg?: tg.Blob.ReadArg): Promise<Uint8Array> {
		return (await this.contents()).read(arg);
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

	export type State = tg.Object.State<File.Id, File.Object>;

	export type Arg =
		| undefined
		| string
		| Uint8Array
		| tg.Blob
		| tg.File
		| tg.File.Arg.Object;

	export namespace Arg {
		export type Object = tg.Graph.Arg.Reference | tg.Graph.Arg.File;
	}

	export type Object = tg.Graph.Reference | tg.Graph.File;

	export namespace Object {
		export let toData = (object: tg.File.Object): tg.File.Data => {
			if ("node" in object) {
				return tg.Graph.Reference.toData(object);
			} else {
				return tg.Graph.File.toData(object);
			}
		};

		export let fromData = (data: tg.File.Data): tg.File.Object => {
			if (tg.Graph.Data.Reference.is(data)) {
				return tg.Graph.Reference.fromData(data);
			} else {
				return tg.Graph.File.fromData(data);
			}
		};

		export let children = (object: tg.File.Object): Array<tg.Object> => {
			if ("node" in object) {
				return tg.Graph.Reference.children(object);
			} else {
				return tg.Graph.File.children(object);
			}
		};
	}

	export type Data = tg.Graph.Data.Reference | tg.Graph.Data.File;

	export let raw = async (
		strings: TemplateStringsArray,
		...placeholders: tg.Args<string>
	): Promise<File> => {
		return await inner(true, strings, ...placeholders);
	};
}
