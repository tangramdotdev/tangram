import { Args } from "./args.ts";
import { Artifact } from "./artifact.ts";
import { assert as assert_, unreachable } from "./assert.ts";
import { Directory } from "./directory.ts";
import type { File } from "./file.ts";
import { mutation } from "./mutation.ts";
import type { Object_ } from "./object.ts";
import { Path } from "./path.ts";
import { type Unresolved, resolve } from "./resolve.ts";
import { Template } from "./template.ts";
import type {
	MaybeMutationMap,
	MaybeNestedArray,
	MutationMap,
} from "./util.ts";

export let symlink = async (
	...args: Array<Unresolved<MaybeNestedArray<MaybeMutationMap<Symlink.Arg>>>>
): Promise<Symlink> => {
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

	static async new(
		...args: Array<Unresolved<MaybeNestedArray<MaybeMutationMap<Symlink.Arg>>>>
	): Promise<Symlink> {
		type Apply = {
			artifact?: Artifact | undefined;
			path?: Array<Path>;
		};
		let { artifact, path: path_ } = await Args.apply<Symlink.Arg, Apply>(
			await Promise.all(args.map(resolve)),
			async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (typeof arg === "string") {
					return {
						path: await mutation({
							kind: "array_append",
							values: [Path.new(arg)],
						}),
					};
				} else if (Artifact.is(arg)) {
					return {
						artifact: arg,
						path: await mutation({ kind: "unset" }),
					};
				} else if (Template.is(arg)) {
					assert_(arg.components.length <= 2);
					let [firstComponent, secondComponent] = arg.components;
					if (
						typeof firstComponent === "string" &&
						secondComponent === undefined
					) {
						return {
							path: await mutation({
								kind: "array_append",
								values: [Path.new(firstComponent)],
							}),
						};
					} else if (
						Artifact.is(firstComponent) &&
						secondComponent === undefined
					) {
						return {
							artifact: firstComponent,
							path: await mutation({ kind: "unset" }),
						};
					} else if (
						Artifact.is(firstComponent) &&
						typeof secondComponent === "string"
					) {
						assert_(secondComponent.startsWith("/"));
						return {
							artifact: firstComponent,
							path: [Path.new(secondComponent.slice(1))],
						};
					} else {
						throw new Error("invalid template");
					}
				} else if (Symlink.is(arg)) {
					let path = await arg.path();
					return {
						artifact: await arg.artifact(),
						path: path !== undefined ? [path] : [],
					};
				} else if (typeof arg === "object") {
					let object: MutationMap<Apply> = {};
					if (arg.artifact !== undefined) {
						object.artifact = arg.artifact;
					}
					if (arg.path !== undefined) {
						object.path = await mutation({ kind: "set", value: [arg.path] });
					}
					return object;
				} else {
					return unreachable();
				}
			},
		);
		let path = path_ !== undefined ? Path.new(path_) : undefined;
		return new Symlink({ object: { artifact, path } });
	}

	static is(value: unknown): value is Symlink {
		return value instanceof Symlink;
	}

	static expect(value: unknown): Symlink {
		assert_(Symlink.is(value));
		return value;
	}

	static assert(value: unknown): asserts value is Symlink {
		assert_(Symlink.is(value));
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

	async resolve(
		from?: Unresolved<Symlink.Arg>,
	): Promise<Directory | File | undefined> {
		from = from ? await symlink(from) : undefined;
		let fromArtifact = await from?.artifact();
		if (Symlink.is(fromArtifact)) {
			fromArtifact = await fromArtifact.resolve();
		}
		let fromPath = await from?.path();
		let artifact = await this.artifact();
		if (Symlink.is(artifact)) {
			artifact = await artifact.resolve();
		}
		let path = await this.path();

		if (artifact !== undefined && fromArtifact !== undefined) {
			throw new Error("expected no `from` value when `artifact` is set");
		}

		if (artifact !== undefined && !path) {
			return artifact;
		} else if (artifact === undefined && path) {
			if (!Directory.is(fromArtifact)) {
				throw new Error("expected a directory");
			}
			return await fromArtifact.tryGet(
				Path.new(fromPath).join("..").join(path).normalize().toString(),
			);
		} else if (artifact !== undefined && path) {
			if (!Directory.is(artifact)) {
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
