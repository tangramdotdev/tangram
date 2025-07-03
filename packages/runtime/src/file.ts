import * as tg from "./index.ts";
import { unindent } from "./template.ts";

export async function file(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<string>
): Promise<File>;
export async function file(...args: tg.Args<File.Arg>): Promise<File>;
export async function file(
	firstArg:
		| TemplateStringsArray
		| tg.Unresolved<tg.ValueOrMaybeMutationMap<File.Arg>>,
	...args: tg.Args<File.Arg>
): Promise<File> {
	return await inner(false, firstArg, ...args);
}

async function inner(
	raw: boolean,
	firstArg:
		| TemplateStringsArray
		| tg.Unresolved<tg.ValueOrMaybeMutationMap<File.Arg>>,
	...args: tg.Args<File.Arg>
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
			if (typeof arg === "object" && "graph" in arg) {
				return File.withObject(
					arg as {
						graph: tg.Graph;
						node: number;
					},
				);
			}
		}
		let arg = await File.arg(...args);
		let contents = await tg.blob(arg.contents);

		let dependencies = Object.fromEntries(
			Object.entries(arg.dependencies ?? {}).map(([key, value]) => {
				if (
					tg.Object.is(value) ||
					typeof value === "number" ||
					"node" in value
				) {
					value = { item: value };
				}
				return [key, value];
			}),
		);
		let executable = arg.executable ?? false;
		const object = { contents, dependencies, executable };
		return File.withObject(object);
	}

	static async arg(
		...args: tg.Args<File.Arg>
	): Promise<Exclude<File.ArgObject, { graph: tg.Graph; node: number }>> {
		type Arg = Exclude<
			File.ArgObject,
			{
				graph: tg.Graph;
				node: number;
			}
		>;
		return await tg.Args.apply<File.Arg, Arg>({
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
					if ("graph" in arg) {
						throw new Error("invalid arg");
					}
					return arg as Arg;
				}
			},
			reduce: {
				contents: (a, b) => tg.blob(a, b),
				dependencies: "merge",
			},
		});
	}

	static expect(value: unknown): File {
		tg.assert(value instanceof File);
		return value;
	}

	static assert(value: unknown): asserts value is File {
		tg.assert(value instanceof File);
	}

	get id(): File.Id {
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
		const object = await this.object();
		if (!("graph" in object)) {
			tg.assert(object.contents, "missing contents");
			return object.contents;
		} else {
			const graph = object.graph;
			const nodes = await graph.nodes();
			const node = nodes[object.node];
			tg.assert(node !== undefined, `invalid index ${object.node}`);
			tg.assert(node.kind === "file", `expected a file node, got ${node}`);
			return node.contents;
		}
	}

	async dependencies(): Promise<{
		[reference: tg.Reference]: tg.Referent<tg.Object>;
	}> {
		const object = await this.object();
		if (!("graph" in object)) {
			const dependencies = object.dependencies;
			return Object.fromEntries(
				await Promise.all(
					Object.entries(dependencies).map(async ([reference, referent]) => {
						let object: tg.Object | undefined;
						tg.assert(typeof referent.item === "object", "expected an object");
						if ("node" in referent.item) {
							tg.assert(referent.item.graph !== undefined, "missing graph");
							object = await referent.item.graph.get(referent.item.node);
						} else {
							object = referent.item;
						}
						const value = {
							...referent,
							item: object,
						};
						return [reference, value];
					}),
				),
			);
		} else {
			const graph = object.graph;
			const nodes = await graph.nodes();
			const node = nodes[object.node];
			tg.assert(node !== undefined, `invalid index ${object.node}`);
			tg.assert(node.kind === "file", `expected a file node, got ${node}`);
			const dependencies = node.dependencies;
			return Object.fromEntries(
				await Promise.all(
					Object.entries(dependencies).map(async ([reference, referent]) => {
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
						const value = {
							...referent,
							item: object,
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
			return Object.values(dependencies).map((d) => d.item);
		}
	}

	async executable(): Promise<boolean> {
		const object = await this.object();
		if (!("graph" in object)) {
			return object.executable;
		} else {
			const graph = object.graph;
			const nodes = await graph.nodes();
			const node = nodes[object.node];
			tg.assert(node !== undefined, `invalid index ${object.node}`);
			tg.assert(node.kind === "file", `expected a file node, got ${node}`);
			return node.executable;
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
	export type Arg =
		| undefined
		| string
		| Uint8Array
		| tg.Blob
		| tg.File
		| ArgObject;

	export type ArgObject =
		| {
				contents?: tg.Blob.Arg | undefined;
				dependencies?:
					| {
							[reference: tg.Reference]: tg.MaybeReferent<
								tg.Graph.Object.Edge<tg.Object>
							>;
					  }
					| undefined;
				executable?: boolean | undefined;
		  }
		| {
				graph: tg.Graph;
				node: number;
		  };

	export type Id = string;

	export type Object =
		| { graph: tg.Graph; node: number }
		| {
				contents?: tg.Blob | undefined;
				dependencies: {
					[reference: tg.Reference]: tg.Referent<
						tg.Graph.Object.Edge<tg.Object>
					>;
				};
				executable: boolean;
		  };

	export namespace Object {
		export let toData = (object: Object): Data => {
			if ("graph" in object) {
				return {
					graph: object.graph.id,
					node: object.node,
				};
			} else {
				return {
					contents: object.contents?.id,
					executable: object.executable,
					dependencies: globalThis.Object.fromEntries(
						globalThis.Object.entries(object.dependencies).map(
							([reference, referent]) => [
								reference,
								tg.Referent.toData(referent, (edge) =>
									tg.Graph.Edge.toData(edge, (object) => object.id),
								),
							],
						),
					),
				};
			}
		};

		export let fromData = (data: Data): Object => {
			if ("graph" in data) {
				return {
					graph: tg.Graph.withId(data.graph),
					node: data.node,
				};
			} else {
				return {
					contents: data.contents ? tg.Blob.withId(data.contents) : undefined,
					executable: data.executable ?? false,
					dependencies: globalThis.Object.fromEntries(
						globalThis.Object.entries(data.dependencies ?? {}).map(
							([reference, referent]) => [
								reference,
								tg.Referent.fromData(referent, (edge) =>
									tg.Graph.Edge.fromData(edge, tg.Object.withId),
								),
							],
						),
					),
				};
			}
		};

		export let children = (object: Object): Array<tg.Object> => {
			if ("graph" in object) {
				return [object.graph];
			} else {
				const contents = object.contents ? [object.contents] : [];
				return [
					...contents,
					...globalThis.Object.entries(object.dependencies).map(
						([_, referent]) => {
							tg.assert(
								typeof referent.item === "object",
								"expected an object",
							);
							if ("node" in referent.item) {
								tg.assert(referent.item.graph !== undefined, "missing graph");
								return referent.item.graph;
							} else {
								return referent.item;
							}
						},
					),
				];
			}
		};
	}

	export type Data =
		| { graph: tg.Graph.Id; node: number }
		| {
				contents?: tg.Blob.Id | undefined;
				dependencies?: {
					[reference: tg.Reference]: tg.Referent.Data<
						tg.Graph.Data.Edge<tg.Object.Id>
					>;
				};
				executable?: boolean;
		  };

	export type State = tg.Object.State<File.Id, File.Object>;

	export let raw = async (
		strings: TemplateStringsArray,
		...placeholders: tg.Args<string>
	): Promise<File> => {
		return await inner(true, strings, ...placeholders);
	};
}
