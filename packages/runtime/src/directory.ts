import * as tg from "./index.ts";

export let directory = async (
	...args: Array<tg.Unresolved<tg.Directory.Arg>>
) => {
	return await tg.Directory.new(...args);
};

export class Directory {
	#state: tg.Directory.State;

	constructor(state: tg.Directory.State) {
		this.#state = state;
	}

	get state(): tg.Directory.State {
		return this.#state;
	}

	static withId(id: tg.Directory.Id): tg.Directory {
		return new tg.Directory({ id, stored: true });
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
			if (
				typeof arg === "object" &&
				"node" in arg &&
				typeof arg.node === "number"
			) {
				return tg.Directory.withObject(arg as tg.Graph.Reference);
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
				for (let [name, entry] of Object.entries(await arg.entries())) {
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
							entries[name] = { node: value };
						} else if (
							typeof value === "object" &&
							typeof value.node === "number"
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
							entries[name] = await Directory.new(existingEntry, value);
						}
					}
				}
			} else {
				return tg.unreachable();
			}
			return entries;
		}, Promise.resolve({}));
		return Directory.withObject({ entries });
	}

	static expect(value: unknown): Directory {
		tg.assert(value instanceof Directory);
		return value;
	}

	static assert(value: unknown): asserts value is Directory {
		tg.assert(value instanceof Directory);
	}

	get id(): Directory.Id {
		if (this.#state.id! !== undefined) {
			return this.#state.id;
		}
		let object = this.#state.object!;
		let data = Directory.Object.toData(object);
		let id = syscall("object_id", { kind: "directory", value: data });
		this.#state.id = id;
		return id;
	}

	async object(): Promise<Directory.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load(): Promise<tg.Directory.Object> {
		if (this.#state.object === undefined) {
			let data = await syscall("object_get", this.#state.id!);
			tg.assert(data.kind === "directory");
			let object = Directory.Object.fromData(data.value);
			this.#state.object = object;
		}
		return this.#state.object!;
	}

	async store(): Promise<tg.Directory.Id> {
		await tg.Value.store(this);
		return this.id;
	}

	async children(): Promise<Array<tg.Object>> {
		let object = await this.load();
		return tg.Directory.Object.children(object);
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
			if (!(artifact instanceof Directory)) {
				return undefined;
			}
			let entries = await artifact.entries();
			let entry: tg.Artifact | undefined = entries[component];
			if (entry === undefined) {
				return undefined;
			}
			parents.push(artifact);
			artifact = entry;
			if (entry instanceof tg.Symlink) {
				let artifact_ = await entry.artifact();
				let path_ = await entry.path();
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

	async entries(): Promise<{ [key: string]: tg.Artifact }> {
		let entries: { [key: string]: tg.Artifact } = {};
		for await (let [name, artifact] of this) {
			entries[name] = artifact;
		}
		return entries;
	}

	async *walk(): AsyncIterableIterator<[string, tg.Artifact]> {
		for await (let [name, artifact] of this) {
			yield [name, artifact];
			if (artifact instanceof Directory) {
				for await (let [entryName, entryArtifact] of artifact.walk()) {
					yield [tg.path.join(name, entryName), entryArtifact];
				}
			}
		}
	}

	async *[Symbol.asyncIterator](): AsyncIterator<[string, tg.Artifact]> {
		let object = await this.object();
		let entries: { [key: string]: tg.Artifact } | undefined;
		if ("entries" in object) {
			entries = Object.fromEntries(
				await Promise.all(
					Object.entries(object.entries).map(async ([name, edge]) => {
						tg.assert(typeof edge === "object", "expected an obejct");
						if ("node" in edge) {
							tg.assert(edge.graph !== undefined, "missing graph");
							let artifact = await edge.graph.get(edge.node);
							return [name, artifact];
						}
						return [name, edge];
					}),
				),
			);
		} else {
			let graph = object.graph;
			tg.assert(graph !== undefined);
			let nodes = await graph.nodes();
			let node = nodes[object.node];
			tg.assert(
				node !== undefined && node.kind === "directory",
				"expected a directory",
			);
			entries = Object.fromEntries(
				await Promise.all(
					Object.entries(node.entries).map(async ([name, edge]) => {
						if (typeof edge === "number") {
							let artifact = await graph.get(edge);
							return [name, artifact];
						} else if ("node" in edge) {
							let artifact = await (edge.graph ?? graph).get(edge.node);
							return [name, artifact];
						}
						return [name, edge];
					}),
				),
			);
		}
		tg.assert(entries !== undefined);
		for (let [name, artifact] of Object.entries(entries)) {
			yield [name, artifact];
		}
	}
}

export namespace Directory {
	export type Id = string;

	export type State = tg.Object.State<tg.Directory.Id, tg.Directory.Object>;

	export type Arg = undefined | tg.Directory | tg.Directory.Arg.Object;

	export namespace Arg {
		export type Object =
			| tg.Graph.Arg.Reference
			| {
					[key: string]:
						| undefined
						| string
						| Uint8Array
						| tg.Blob
						| tg.Artifact
						| tg.Directory.Arg.Object;
			  };
	}

	export type Object = tg.Graph.Reference | tg.Graph.Directory;

	export namespace Object {
		export let toData = (object: tg.Directory.Object): tg.Directory.Data => {
			if ("node" in object) {
				return tg.Graph.Reference.toData(object);
			} else {
				return tg.Graph.Directory.toData(object);
			}
		};

		export let fromData = (data: tg.Directory.Data): tg.Directory.Object => {
			if (tg.Graph.Data.Reference.is(data)) {
				return tg.Graph.Reference.fromData(data);
			} else {
				return tg.Graph.Directory.fromData(data);
			}
		};

		export let children = (object: tg.Directory.Object): Array<tg.Object> => {
			if ("node" in object) {
				return tg.Graph.Reference.children(object);
			} else {
				return tg.Graph.Directory.children(object);
			}
		};
	}

	export type Data = tg.Graph.Data.Reference | tg.Graph.Data.Directory;
}
