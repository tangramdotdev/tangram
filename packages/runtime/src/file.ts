import * as tg from "./index.ts";
import { flatten } from "./util.ts";

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
		let contents = await tg.blob(arg.contents);
		let dependencies = arg.dependencies ?? {};
		let executable = arg.executable ?? false;
		return new File({
			object: { contents, dependencies, executable },
		});
	}

	static async arg(...args: tg.Args<File.Arg>): Promise<File.ArgObject> {
		let resolved = await Promise.all(args.map(tg.resolve));
		let flattened = flatten(resolved);
		let objects = await Promise.all(
			flattened.map(async (arg) => {
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
		let mutations = await tg.Args.createMutations(objects, {
			contents: "append",
			dependencies: "append",
		});
		let arg = await tg.Args.applyMutations(mutations);
		if (arg.dependencies !== undefined) {
			let dependencies: { [reference: string]: File.Dependency } = {};
			let allDependencies = arg.dependencies as Array<{
				[reference: string]: File.Dependency;
			}>;
			for (let dependencyMap of allDependencies) {
				for (let [reference, dependency] of Object.entries(dependencyMap)) {
					dependencies = { ...dependencies, [reference]: dependency };
				}
			}
			arg.dependencies = dependencies;
		}
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
			let object = await syscall("load", this.#state.id!);
			tg.assert(object.kind === "file");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("store", {
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

	async dependencies(): Promise<
		{ [reference: string]: File.Dependency } | undefined
	> {
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
			if (dependencies === undefined) {
				return undefined;
			}
			return Object.fromEntries(
				Object.entries(dependencies).map(([reference, dependency]) => {
					let object: tg.Object | undefined;
					if (typeof dependency.object === "number") {
						const node = nodes[dependency.object];
						tg.assert(node !== undefined, `invalid index ${dependency.object}`);
						switch (node.kind) {
							case "directory": {
								object = new tg.Directory({
									object: { graph, node: dependency.object },
								});
								break;
							}
							case "file": {
								object = new tg.File({
									object: { graph, node: dependency.object },
								});
								break;
							}
							case "symlink": {
								object = new tg.Symlink({
									object: { graph, node: dependency.object },
								});
								break;
							}
						}
					} else {
						object = dependency.object;
					}
					const value = {
						...dependency,
						object,
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
			return Object.values(dependencies).map((d) => d.object);
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

	async size(): Promise<number> {
		return (await this.contents()).size();
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

	export type ArgObject = {
		contents?: tg.Blob.Arg | undefined;
		dependencies?: { [reference: string]: Dependency } | undefined;
		executable?: boolean | undefined;
	};

	export type Id = string;

	export type Object =
		| {
				contents: tg.Blob;
				dependencies: { [reference: string]: Dependency } | undefined;
				executable: boolean;
		  }
		| { graph: tg.Graph; node: number };

	export type Dependency = {
		object: tg.Object;
		tag?: string | undefined;
	};

	export type State = tg.Object.State<File.Id, File.Object>;
}
