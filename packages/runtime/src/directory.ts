import * as tg from "./index.ts";
import type { MaybeNestedArray } from "./util.ts";

export let directory = async (
	...args: Array<tg.Unresolved<MaybeNestedArray<Directory.Arg>>>
) => {
	return await Directory.new(...args);
};

export class Directory {
	#state: Directory.State;

	constructor(state: Directory.State) {
		this.#state = state;
	}

	get state(): Directory.State {
		return this.#state;
	}

	static withId(id: Directory.Id): Directory {
		return new Directory({ id });
	}

	static async new(
		...args: Array<tg.Unresolved<MaybeNestedArray<Directory.Arg>>>
	): Promise<Directory> {
		let entries = await (await Promise.all(args.map(tg.resolve))).reduce<
			Promise<{ [key: string]: tg.Artifact }>
		>(async function reduce(promiseEntries, arg) {
			let entries = await promiseEntries;
			if (arg === undefined) {
				// If the arg is undefined, then continue.
			} else if (arg instanceof Directory) {
				// If the arg is a directory, then apply each entry.
				for (let [name, entry] of Object.entries(await arg.entries())) {
					// Get an existing entry.
					let existingEntry = entries[name];

					// Merge the existing entry with the entry if they are both directories.
					if (
						existingEntry instanceof Directory &&
						entry instanceof Directory
					) {
						entry = await Directory.new(existingEntry, entry);
					}

					// Set the entry.
					entries[name] = entry;
				}
			} else if (arg instanceof Array) {
				for (let argEntry of arg) {
					entries = await reduce(Promise.resolve(entries), argEntry);
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
					if (!(existingEntry instanceof Directory)) {
						existingEntry = undefined;
					}

					if (trailingComponents.length > 0) {
						// If there are trailing path components, then recurse.
						let trailingPath = tg.path.fromComponents(trailingComponents);

						// Merge the entry with the trailing path.
						let newEntry = await Directory.new(existingEntry, {
							[trailingPath]: value,
						});

						// Add the entry.
						entries[name] = newEntry;
					} else {
						// If there are no trailing path components, then create the artifact specified by the value.
						if (value === undefined) {
							delete entries[name];
						} else if (
							typeof value === "string" ||
							value instanceof Uint8Array ||
							tg.Blob.is(value)
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
		return new Directory({ object: { entries } });
	}

	static expect(value: unknown): Directory {
		tg.assert(value instanceof Directory);
		return value;
	}

	static assert(value: unknown): asserts value is Directory {
		tg.assert(value instanceof Directory);
	}

	async id(): Promise<Directory.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<Directory.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("load", this.#state.id!);
			tg.assert(object.kind === "directory");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("store", {
				kind: "directory",
				value: this.#state.object!,
			});
		}
	}

	async get(arg: string): Promise<Directory | tg.File> {
		let artifact = await this.tryGet(arg);
		tg.assert(artifact, `Failed to get the directory entry "${arg}".`);
		return artifact;
	}

	async tryGet(arg: string): Promise<Directory | tg.File | undefined> {
		let artifact: Directory | tg.File = this;
		let currentPath = "";
		for (let component of tg.path.components(arg)) {
			if (!tg.path.Component.isNormal(component)) {
				throw new Error("all path components must be normal");
			}
			if (!(artifact instanceof Directory)) {
				return undefined;
			}
			currentPath = tg.path.join(currentPath, component);
			let entry: tg.Artifact | undefined = (await artifact.entries())[
				component
			];
			if (entry === undefined) {
				return undefined;
			} else if (entry instanceof tg.Symlink) {
				let resolved = await entry.resolve({
					artifact: this,
					path: currentPath,
				});
				if (resolved === undefined) {
					return undefined;
				}
				artifact = resolved;
			} else {
				artifact = entry;
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
		for (let [name, artifact] of Object.entries(object.entries)) {
			yield [name, artifact];
		}
	}
}

export namespace Directory {
	export type Arg = undefined | Directory | ArgObject;

	type ArgObject = {
		[key: string]:
			| undefined
			| string
			| Uint8Array
			| tg.Blob
			| tg.Artifact
			| ArgObject;
	};

	export type Id = string;

	export type Object = {
		entries: { [key: string]: tg.Artifact };
	};

	export type State = tg.Object.State<Directory.Id, Directory.Object>;
}
