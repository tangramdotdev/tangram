import * as tg from "./index.ts";

export let directory = async (
	...args: Array<tg.Unresolved<tg.Directory.Arg>>
) => {
	return await tg.Directory.new(...args);
};

export class Directory {
	#state: tg.Object.State;

	constructor(arg: {
		id?: tg.Directory.Id;
		object?: tg.Directory.Object;
		stored: boolean;
	}) {
		let object =
			arg.object !== undefined
				? { kind: "directory" as const, value: arg.object }
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

	static withId(id: tg.Directory.Id): tg.Directory {
		return new tg.Directory({ id, stored: true });
	}

	static withPointer(pointer: tg.Graph.Pointer): tg.Directory {
		return new tg.Directory({ object: pointer, stored: false });
	}

	static withObject(object: tg.Directory.Object): tg.Directory {
		return new tg.Directory({ object, stored: false });
	}

	static fromData(data: tg.Directory.Data): tg.Directory {
		return tg.Directory.withObject(tg.Directory.Object.fromData(data));
	}

	static async new(
		...args: Array<tg.Unresolved<tg.Directory.Arg>>
	): Promise<tg.Directory> {
		if (args.length === 1) {
			let arg = await tg.resolve(args[0]);
			if (tg.Graph.Arg.Pointer.is(arg)) {
				return tg.Directory.withObject(tg.Graph.Pointer.fromArg(arg));
			}
			if (tg.Directory.Arg.Branch.is(arg)) {
				let children: Array<tg.Graph.DirectoryChild> = [];
				for (let child of arg.children) {
					let directory = await tg.Directory.new(child.directory);
					children.push({
						directory,
						count: child.count,
						last: child.last,
					});
				}
				return tg.Directory.withObject({ children });
			}
		}
		let resolved = await Promise.all(args.map(tg.resolve));
		let entries = await resolved.reduce<
			Promise<{ [key: string]: tg.Graph.Edge<tg.Artifact> }>
		>(async function reduce(promise, arg) {
			let entries = await promise;
			if (arg === undefined) {
				// If the arg is undefined, then continue.
			} else if (arg instanceof tg.Directory) {
				// If the arg is a directory, then apply each entry.
				for (let [name, entry] of Object.entries(await arg.entries)) {
					// Get an existing entry.
					let existingEntry = entries[name];

					// Merge the existing entry with the entry if they are both directories.
					if (
						existingEntry instanceof tg.Directory &&
						entry instanceof tg.Directory
					) {
						entry = await tg.Directory.new(existingEntry, entry);
					}

					// Set the entry.
					entries[name] = entry;
				}
			} else if (typeof arg === "object") {
				// If the arg is an object, then apply each entry.
				for (let [key, value] of Object.entries(arg)) {
					// Separate the first normal path component from the trailing path components.
					let [firstComponent, ...trailingComponents] = tg.path.components(key);
					if (firstComponent === undefined) {
						throw new Error("the path must have at least one component");
					}
					if (!tg.path.Component.isNormal(firstComponent)) {
						throw new Error("all path components must be normal");
					}
					let name = firstComponent;

					// Get an existing entry.
					let existingEntry = entries[name];

					// Remove the entry if it is not a directory.
					if (!(existingEntry instanceof tg.Directory)) {
						existingEntry = undefined;
					}

					if (trailingComponents.length > 0) {
						// If there are trailing path components, then recurse.
						let trailingPath = tg.path.fromComponents(trailingComponents);

						// Merge the entry with the trailing path.
						let newEntry = await tg.Directory.new(existingEntry, {
							[trailingPath]: value,
						});

						// Add the entry.
						entries[name] = newEntry;
					} else {
						// If there are no trailing path components, then create the artifact specified by the value.
						if (value === undefined) {
							delete entries[name];
						} else if (typeof value === "number") {
							throw new Error(
								"cannot use number as directory entry without kind",
							);
						} else if (
							typeof value === "object" &&
							typeof value.index === "number"
						) {
							entries[name] = value;
						} else if (
							typeof value === "string" ||
							value instanceof Uint8Array ||
							value instanceof tg.Blob
						) {
							let newEntry = await tg.file(value);
							entries[name] = newEntry;
						} else if (
							value instanceof tg.File ||
							value instanceof tg.Symlink
						) {
							entries[name] = value;
						} else {
							entries[name] = await tg.Directory.new(existingEntry, value);
						}
					}
				}
			} else {
				return tg.unreachable();
			}
			return entries;
		}, Promise.resolve({}));
		return tg.Directory.withObject({ entries });
	}

	static expect(value: unknown): tg.Directory {
		tg.assert(value instanceof tg.Directory);
		return value;
	}

	static assert(value: unknown): asserts value is tg.Directory {
		tg.assert(value instanceof tg.Directory);
	}

	get id(): tg.Directory.Id {
		let id = this.#state.id;
		tg.assert(tg.Object.Id.kind(id) === "directory");
		return id;
	}

	async object(): Promise<tg.Directory.Object> {
		let object = await this.#state.load();
		tg.assert(object.kind === "directory");
		return object.value;
	}

	async load(): Promise<tg.Directory.Object> {
		let object = await this.#state.load();
		tg.assert(object.kind === "directory");
		return object.value;
	}

	unload(): void {
		this.#state.unload();
	}

	async store(): Promise<tg.Directory.Id> {
		await tg.Value.store(this);
		return this.id;
	}

	async children(): Promise<Array<tg.Object>> {
		return this.#state.children();
	}

	async get(arg: string): Promise<tg.Artifact> {
		let artifact = await this.tryGet(arg);
		tg.assert(artifact, `Failed to get the directory entry "${arg}".`);
		return artifact;
	}

	async tryGet(arg: string): Promise<tg.Artifact | undefined> {
		let components = tg.path.components(arg);
		let artifact: tg.Artifact = this;
		let parents: Array<tg.Directory> = [];
		while (true) {
			let component = components.shift();
			if (component === undefined) {
				break;
			} else if (component === tg.path.Component.Root) {
				throw new Error("invalid path");
			} else if (component === ".") {
				continue;
			} else if (component === "..") {
				let parent = parents.pop();
				if (!parent) {
					throw new Error("path is external");
				}
				artifact = parent;
				continue;
			}
			if (!(artifact instanceof tg.Directory)) {
				return undefined;
			}
			let entries: { [key: string]: tg.Artifact } = await artifact.entries;
			let entry: tg.Artifact | undefined = entries[component];
			if (entry === undefined) {
				return undefined;
			}
			parents.push(artifact);
			artifact = entry;
			if (entry instanceof tg.Symlink) {
				let artifact_ = await entry.artifact;
				let path_ = await entry.path;
				if (artifact_ === undefined && path_ !== undefined) {
					let parent = parents.pop();
					if (!parent) {
						throw new Error("path is external");
					}
					artifact = parent;
					components.unshift(...tg.path.components(path_));
				} else if (artifact_ !== undefined && path_ === undefined) {
					return artifact_;
				} else if (artifact_ instanceof tg.Directory && path_ !== undefined) {
					return await artifact_.tryGet(path_);
				} else {
					throw new Error("invalid symlink");
				}
			}
		}
		return artifact;
	}

	get entries(): Promise<{ [key: string]: tg.Artifact }> {
		return (async () => {
			let entries: { [key: string]: tg.Artifact } = {};
			for await (let [name, artifact] of this) {
				entries[name] = artifact;
			}
			return entries;
		})();
	}

	async *walk(): AsyncIterableIterator<[string, tg.Artifact]> {
		for await (let [name, artifact] of this) {
			yield [name, artifact];
			if (artifact instanceof tg.Directory) {
				for await (let [entryName, entryArtifact] of artifact.walk()) {
					yield [tg.path.join(name, entryName), entryArtifact];
				}
			}
		}
	}

	async *[Symbol.asyncIterator](): AsyncIterator<[string, tg.Artifact]> {
		let object = await this.object();
		if (tg.Graph.Pointer.is(object)) {
			let graph = object.graph;
			tg.assert(graph !== undefined);
			let nodes = await graph.nodes;
			let node = nodes[object.index];
			tg.assert(
				node !== undefined && node.kind === "directory",
				"expected a directory",
			);
			if ("entries" in node) {
				for (let [name, edge] of Object.entries(node.entries)) {
					if (typeof edge === "number") {
						let artifact = await graph.get(edge);
						yield [name, artifact];
					} else if ("index" in edge) {
						let artifact = await (edge.graph ?? graph).get(edge.index);
						yield [name, artifact];
					} else {
						yield [name, edge];
					}
				}
			} else {
				for (let child of node.children) {
					let childDir = await Directory.resolveEdgeInGraph(
						child.directory,
						graph,
					);
					for await (let entry of childDir) {
						yield entry;
					}
				}
			}
		} else if (tg.Graph.Directory.isLeaf(object)) {
			for (let [name, edge] of Object.entries(object.entries)) {
				tg.assert(typeof edge === "object", "expected an object");
				if (tg.Graph.Pointer.is(edge)) {
					tg.assert(edge.graph !== undefined, "missing graph");
					let artifact = await edge.graph.get(edge.index);
					yield [name, artifact];
				} else {
					yield [name, edge];
				}
			}
		} else {
			for (let child of object.children) {
				let childDir = await Directory.resolveEdge(child.directory);
				for await (let entry of childDir) {
					yield entry;
				}
			}
		}
	}

	static async resolveEdge(
		edge: tg.Graph.Edge<tg.Directory>,
	): Promise<tg.Directory> {
		if (tg.Graph.Pointer.is(edge)) {
			tg.assert(edge.graph !== undefined, "missing graph for directory edge");
			let artifact = await edge.graph.get(edge.index);
			tg.assert(artifact instanceof tg.Directory, "expected a directory");
			return artifact;
		} else {
			return edge;
		}
	}

	static async resolveEdgeInGraph(
		edge: tg.Graph.Edge<tg.Directory>,
		graph: tg.Graph,
	): Promise<tg.Directory> {
		if (tg.Graph.Pointer.is(edge)) {
			let g = edge.graph ?? graph;
			let artifact = await g.get(edge.index);
			tg.assert(artifact instanceof tg.Directory, "expected a directory");
			return artifact;
		} else {
			return edge;
		}
	}
}

export namespace Directory {
	export type Id = string;

	export type Arg = undefined | tg.Directory | tg.Directory.Arg.Object;

	export namespace Arg {
		export type Object =
			| tg.Graph.Arg.Pointer
			| tg.Directory.Arg.Leaf
			| tg.Directory.Arg.Branch;

		export type Leaf = {
			[key: string]:
				| undefined
				| string
				| Uint8Array
				| tg.Blob
				| tg.Artifact
				| tg.Directory.Arg.Object;
		};

		export type Branch = {
			children: Array<tg.Directory.Arg.Child>;
		};

		export namespace Branch {
			export let is = (value: unknown): value is tg.Directory.Arg.Branch => {
				return (
					typeof value === "object" &&
					value !== null &&
					"children" in value &&
					Array.isArray((value as tg.Directory.Arg.Branch).children)
				);
			};
		}

		export type Child = {
			directory: tg.Directory.Arg.Object | tg.Directory;
			count: number;
			last: string;
		};
	}

	export type Object = tg.Graph.Pointer | tg.Graph.Directory;

	export namespace Object {
		export let toData = (object: tg.Directory.Object): tg.Directory.Data => {
			if (tg.Graph.Pointer.is(object)) {
				return tg.Graph.Pointer.toData(object);
			} else {
				return tg.Graph.Directory.toData(object);
			}
		};

		export let fromData = (data: tg.Directory.Data): tg.Directory.Object => {
			if (tg.Graph.Data.Pointer.is(data)) {
				return tg.Graph.Pointer.fromData(data);
			} else {
				return tg.Graph.Directory.fromData(data);
			}
		};

		export let children = (object: tg.Directory.Object): Array<tg.Object> => {
			if (tg.Graph.Pointer.is(object)) {
				return tg.Graph.Pointer.children(object);
			} else {
				return tg.Graph.Directory.children(object);
			}
		};
	}

	export type Data = tg.Graph.Data.Pointer | tg.Graph.Data.Directory;
}
