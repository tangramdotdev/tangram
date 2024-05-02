import type { Artifact } from "./artifact.ts";
import { assert as assert_, unreachable } from "./assert.ts";
import { Blob } from "./blob.ts";
import { File, file } from "./file.ts";
import type { Object_ } from "./object.ts";
import { Path } from "./path.ts";
import { type Unresolved, resolve } from "./resolve.ts";
import { Symlink } from "./symlink.ts";
import type { MaybeNestedArray } from "./util.ts";

export let directory = async (
	...args: Array<Unresolved<MaybeNestedArray<Directory.Arg>>>
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
		...args: Array<Unresolved<MaybeNestedArray<Directory.Arg>>>
	): Promise<Directory> {
		let entries = await (await Promise.all(args.map(resolve))).reduce<
			Promise<Record<string, Artifact>>
		>(async function reduce(promiseEntries, arg) {
			let entries = await promiseEntries;
			if (arg === undefined) {
				// If the arg is undefined, then continue.
			} else if (Directory.is(arg)) {
				// If the arg is a directory, then apply each entry.
				for (let [name, entry] of Object.entries(await arg.entries())) {
					// Get an existing entry.
					let existingEntry = entries[name];

					// Merge the existing entry with the entry if they are both directories.
					if (Directory.is(existingEntry) && Directory.is(entry)) {
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
					let [_, firstComponent, ...trailingComponents] =
						Path.new(key).components;
					if (firstComponent === undefined) {
						throw new Error("the path must have at least one component");
					}
					if (!Path.Component.isNormal(firstComponent)) {
						throw new Error("all path components must be normal");
					}
					let name = firstComponent;

					// Get an existing entry.
					let existingEntry = entries[name];

					// Remove the entry if it is not a directory.
					if (!Directory.is(existingEntry)) {
						existingEntry = undefined;
					}

					if (trailingComponents.length > 0) {
						// If there are trailing path components, then recurse.
						let trailingPath = Path.new(trailingComponents).toString();

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
							Blob.is(value)
						) {
							let newEntry = await file(value);
							entries[name] = newEntry;
						} else if (File.is(value) || Symlink.is(value)) {
							entries[name] = value;
						} else {
							entries[name] = await Directory.new(existingEntry, value);
						}
					}
				}
			} else {
				return unreachable();
			}
			return entries;
		}, Promise.resolve({}));
		return new Directory({ object: { entries } });
	}

	static is(value: unknown): value is Directory {
		return value instanceof Directory;
	}

	static expect(value: unknown): Directory {
		assert_(Directory.is(value));
		return value;
	}

	static assert(value: unknown): asserts value is Directory {
		assert_(Directory.is(value));
	}

	async id(): Promise<Directory.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<Directory.Object_> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("load", this.#state.id!);
			assert_(object.kind === "directory");
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

	async get(arg: Path.Arg): Promise<Directory | File> {
		let artifact = await this.tryGet(arg);
		assert_(artifact, `Failed to get the directory entry "${arg}".`);
		return artifact;
	}

	async tryGet(arg: Path.Arg): Promise<Directory | File | undefined> {
		let artifact: Directory | File = this;
		let currentPath = Path.new();
		let components = Path.new(arg).components;
		for (let i = 1; i < components.length; i++) {
			let component = components[i]!;
			if (!Path.Component.isNormal(component)) {
				throw new Error("all path components must be normal");
			}
			if (!Directory.is(artifact)) {
				return undefined;
			}
			currentPath.push(component);
			let entry: Artifact | undefined = (await artifact.entries())[component];
			if (entry === undefined) {
				return undefined;
			} else if (Symlink.is(entry)) {
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

	async entries(): Promise<Record<string, Artifact>> {
		let entries: Record<string, Artifact> = {};
		for await (let [name, artifact] of this) {
			entries[name] = artifact;
		}
		return entries;
	}

	async archive(format: Artifact.ArchiveFormat): Promise<Blob> {
		return await syscall("archive", this, format);
	}

	async bundle(): Promise<Directory> {
		return await syscall("bundle", this);
	}

	async *walk(): AsyncIterableIterator<[Path, Artifact]> {
		for await (let [name, artifact] of this) {
			yield [Path.new(name), artifact];
			if (Directory.is(artifact)) {
				for await (let [entryName, entryArtifact] of artifact.walk()) {
					yield [Path.new(name).join(entryName), entryArtifact];
				}
			}
		}
	}

	async *[Symbol.asyncIterator](): AsyncIterator<[string, Artifact]> {
		let object = await this.object();
		for (let [name, artifact] of Object.entries(object.entries)) {
			yield [name, artifact];
		}
	}
}

export namespace Directory {
	export type Arg = undefined | Directory | ArgObject;

	type ArgObject = {
		[name: string]:
			| undefined
			| string
			| Uint8Array
			| Blob
			| Artifact
			| ArgObject;
	};

	export type Id = string;

	export type Object_ = {
		entries: { [key: string]: Artifact };
	};

	export type State = Object_.State<Directory.Id, Directory.Object_>;
}
