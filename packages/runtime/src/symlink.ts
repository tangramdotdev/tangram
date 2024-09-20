import * as tg from "./index.ts";
import { flatten } from "./util.ts";

export let symlink = async (
	...args: tg.Args<Symlink.Arg>
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

	static async new(...args: tg.Args<Symlink.Arg>): Promise<Symlink> {
		let arg = await Symlink.arg(...args);
		let artifact = arg.artifact;
		let path = arg.path !== undefined ? arg.path : undefined;
		let object = { artifact, path };
		return new Symlink({ object });
	}

	static async arg(...args: tg.Args<Symlink.Arg>): Promise<Symlink.ArgObject> {
		let resolved = await Promise.all(args.map(tg.resolve));
		let flattened = flatten(resolved);
		let objects = await Promise.all(
			flattened.map(async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (typeof arg === "string") {
					return { path: arg };
				} else if (tg.Artifact.is(arg)) {
					return { artifact: arg, path: tg.Mutation.unset() };
				} else if (arg instanceof tg.Template) {
					tg.assert(arg.components.length <= 2);
					let [firstComponent, secondComponent] = arg.components;
					if (
						typeof firstComponent === "string" &&
						secondComponent === undefined
					) {
						return { path: firstComponent };
					} else if (
						tg.Artifact.is(firstComponent) &&
						secondComponent === undefined
					) {
						return { artifact: firstComponent, path: tg.Mutation.unset() };
					} else if (
						tg.Artifact.is(firstComponent) &&
						typeof secondComponent === "string"
					) {
						tg.assert(secondComponent.startsWith("/"));
						return {
							artifact: firstComponent,
							path: secondComponent.slice(1),
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
		let mutations = await tg.Args.createMutations(objects, {
			path: "append",
		});
		let arg = await tg.Args.applyMutations(mutations);
		return arg;
	}

	static expect(value: unknown): Symlink {
		tg.assert(value instanceof Symlink);
		return value;
	}

	static assert(value: unknown): asserts value is Symlink {
		tg.assert(value instanceof Symlink);
	}

	async id(): Promise<Symlink.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<Symlink.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("load", this.#state.id!);
			tg.assert(object.kind === "symlink");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("store", {
				kind: "symlink",
				value: this.#state.object!,
			});
		}
	}

	async artifact(): Promise<tg.Artifact | undefined> {
		let object = await this.object();
		return object.artifact;
	}

	async path(): Promise<string | undefined> {
		let object = await this.object();
		return object.path;
	}

	async resolve(
		from?: Symlink.Arg,
	): Promise<tg.Directory | tg.File | undefined> {
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
			if (!(fromArtifact instanceof tg.Directory)) {
				throw new Error("expected a directory");
			}

			return await fromArtifact.tryGet(
				tg.Path.normalize(tg.Path.join(fromPath, "..", path)),
			);
		} else if (artifact !== undefined && path) {
			if (!(artifact instanceof tg.Directory)) {
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
		| tg.Artifact
		| tg.Template
		| Symlink
		| ArgObject;

	export type ArgObject = {
		artifact?: tg.Artifact | undefined;
		path?: string | undefined;
	};

	export type Id = string;

	export type Object = {
		artifact: tg.Artifact | undefined;
		path: string | undefined;
	};

	export type State = tg.Object.State<Symlink.Id, Symlink.Object>;
}
