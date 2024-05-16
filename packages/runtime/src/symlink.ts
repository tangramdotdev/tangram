import { Args } from "./args.ts";
import { Artifact } from "./artifact.ts";
import { assert as assert_ } from "./assert.ts";
import { Directory } from "./directory.ts";
import type { File } from "./file.ts";
import { Mutation } from "./mutation.ts";
import type { Object_ } from "./object.ts";
import { Path } from "./path.ts";
import { resolve } from "./resolve.ts";
import { Template } from "./template.ts";
import { flatten } from "./util.ts";

export let symlink = async (...args: Args<Symlink.Arg>): Promise<Symlink> => {
	return await Symlink.new(...args);
};

export class Symlink {
	#state: Symlink.State;

	constructor(state: Symlink.State) {
		this.#state = state;
	}

	get state(): Symlink.State {
		return this.#state;
	}

	static withId(id: Symlink.Id): Symlink {
		return new Symlink({ id });
	}

	static async new(...args: Args<Symlink.Arg>): Promise<Symlink> {
		let arg = await Symlink.arg(...args);
		let artifact = arg.artifact;
		let path = arg.path !== undefined ? Path.new(arg.path) : undefined;
		let object = { artifact, path };
		return new Symlink({ object });
	}

	static async arg(...args: Args<Symlink.Arg>): Promise<Symlink.ArgObject> {
		let resolved = await Promise.all(args.map(resolve));
		let flattened = flatten(resolved);
		let objects = await Promise.all(
			flattened.map(async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (typeof arg === "string") {
					return { path: Path.new(arg) };
				} else if (Artifact.is(arg)) {
					return { artifact: arg, path: Mutation.unset() };
				} else if (arg instanceof Template) {
					assert_(arg.components.length <= 2);
					let [firstComponent, secondComponent] = arg.components;
					if (
						typeof firstComponent === "string" &&
						secondComponent === undefined
					) {
						return { path: Path.new(firstComponent) };
					} else if (
						Artifact.is(firstComponent) &&
						secondComponent === undefined
					) {
						return { artifact: firstComponent, path: Mutation.unset() };
					} else if (
						Artifact.is(firstComponent) &&
						typeof secondComponent === "string"
					) {
						assert_(secondComponent.startsWith("/"));
						return {
							artifact: firstComponent,
							path: Path.new(secondComponent.slice(1)),
						};
					} else {
						throw new Error("invalid template");
					}
				} else if (arg instanceof Symlink) {
					let path = await arg.path();
					return { artifact: await arg.artifact(), path };
				} else {
					return arg;
				}
			}),
		);
		let mutations = await Args.createMutations(objects, {
			path: "array_append",
		});
		let arg = await Args.applyMutations(mutations);
		return arg;
	}

	static expect(value: unknown): Symlink {
		assert_(value instanceof Symlink);
		return value;
	}

	static assert(value: unknown): asserts value is Symlink {
		assert_(value instanceof Symlink);
	}

	async id(): Promise<Symlink.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<Symlink.Object_> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("object_load", this.#state.id!);
			assert_(object.kind === "symlink");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("object_store", {
				kind: "symlink",
				value: this.#state.object!,
			});
		}
	}

	async artifact(): Promise<Artifact | undefined> {
		let object = await this.object();
		return object.artifact;
	}

	async path(): Promise<Path | undefined> {
		let object = await this.object();
		return object.path;
	}

	async resolve(from?: Symlink.Arg): Promise<Directory | File | undefined> {
		from = from ? await symlink(from) : undefined;
		let fromArtifact = await from?.artifact();
		if (fromArtifact instanceof Symlink) {
			fromArtifact = await fromArtifact.resolve();
		}
		let fromPath = await from?.path();
		let artifact = await this.artifact();
		if (artifact instanceof Symlink) {
			artifact = await artifact.resolve();
		}
		let path = await this.path();

		if (artifact !== undefined && fromArtifact !== undefined) {
			throw new Error("expected no `from` value when `artifact` is set");
		}

		if (artifact !== undefined && !path) {
			return artifact;
		} else if (artifact === undefined && path) {
			if (!(fromArtifact instanceof Directory)) {
				throw new Error("expected a directory");
			}
			return await fromArtifact.tryGet(
				Path.new(fromPath).join("..").join(path).normalize().toString(),
			);
		} else if (artifact !== undefined && path) {
			if (!(artifact instanceof Directory)) {
				throw new Error("expected a directory");
			}
			return await artifact.tryGet(path);
		} else {
			throw new Error("invalid symlink");
		}
	}
}

export namespace Symlink {
	export type Arg =
		| undefined
		| string
		| Artifact
		| Template
		| Symlink
		| ArgObject;

	export type ArgObject = {
		artifact?: Artifact | undefined;
		path?: Path | undefined;
	};

	export type Id = string;

	export type Object_ = {
		artifact: Artifact | undefined;
		path: Path | undefined;
	};

	export type State = Object_.State<Symlink.Id, Symlink.Object_>;
}
