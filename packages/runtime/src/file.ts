import * as tg from "./index.ts";

export let file = async (...args: tg.Args<File.Arg>) => {
	return await File.new(...args);
};

export class File {
	#state: File.State;

	constructor(state: File.State) {
		this.#state = state;
	}

	get state(): File.State {
		return this.#state;
	}

	static withId(id: File.Id): File {
		return new File({ id });
	}

	static async new(...args: tg.Args<File.Arg>): Promise<File> {
		let arg = await File.arg(...args);
		if ("graph" in arg) {
			return new File({ object: arg });
		}
		let contents = await tg.blob(arg.contents);
		let dependencies = arg.dependencies ?? {};
		let executable = arg.executable ?? false;
		const object = { contents, dependencies, executable };
		return new File({
			object,
		});
	}

	static async arg(...args: tg.Args<File.Arg>): Promise<File.ArgObject> {
		let resolved = await Promise.all(args.map(tg.resolve));
		if (resolved.length === 1) {
			const arg = resolved[0];
			if (typeof arg === "object" && "graph" in arg) {
				return arg;
			}
		}
		if (resolved.some((arg) => typeof arg === "object" && "graph" in arg)) {
			throw new Error("only a single graph arg is supported");
		}
		let objects = await Promise.all(
			resolved.map(async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (
					typeof arg === "string" ||
					arg instanceof Uint8Array ||
					tg.Blob.is(arg)
				) {
					return { contents: arg };
				} else if (arg instanceof File) {
					return {
						contents: await arg.contents(),
						dependencies: await arg.dependencies(),
					};
				} else {
					return arg;
				}
			}),
		);
		let arg = await tg.Args.apply(objects, {
			contents: "append",
			dependencies: "merge",
		});
		return arg;
	}

	static expect(value: unknown): File {
		tg.assert(value instanceof File);
		return value;
	}

	static assert(value: unknown): asserts value is File {
		tg.assert(value instanceof File);
	}

	async id(): Promise<File.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<File.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("object_load", this.#state.id!);
			tg.assert(object.kind === "file");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("object_store", {
				kind: "file",
				value: this.#state.object!,
			});
		}
	}

	async contents(): Promise<tg.Blob> {
		const object = await this.object();
		if (!("graph" in object)) {
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
			return object.dependencies;
		} else {
			const graph = object.graph;
			const nodes = await graph.nodes();
			const node = nodes[object.node];
			tg.assert(node !== undefined, `invalid index ${object.node}`);
			tg.assert(node.kind === "file", `expected a file node, got ${node}`);
			const dependencies = node.dependencies;
			return Object.fromEntries(
				Object.entries(dependencies).map(([reference, referent]) => {
					let object: tg.Object | undefined;
					if (typeof referent.item === "number") {
						const node = nodes[referent.item];
						tg.assert(node !== undefined, `invalid index ${referent.item}`);
						switch (node.kind) {
							case "directory": {
								object = new tg.Directory({
									object: { graph, node: referent.item },
								});
								break;
							}
							case "file": {
								object = new tg.File({
									object: { graph, node: referent.item },
								});
								break;
							}
							case "symlink": {
								object = new tg.Symlink({
									object: { graph, node: referent.item },
								});
								break;
							}
						}
					} else {
						object = referent.item;
					}
					const value = {
						...referent,
						item: object,
					};
					return [reference, value];
				}),
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
					| { [reference: tg.Reference]: tg.Referent<tg.Object> }
					| undefined;
				executable?: boolean | undefined;
		  }
		| { graph: tg.Graph; node: number };

	export type Id = string;

	export type Object =
		| {
				contents: tg.Blob;
				dependencies: { [reference: tg.Reference]: tg.Referent<tg.Object> };
				executable: boolean;
		  }
		| { graph: tg.Graph; node: number };

	export type State = tg.Object.State<File.Id, File.Object>;
}
