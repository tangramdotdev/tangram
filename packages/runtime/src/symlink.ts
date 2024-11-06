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
		if ("graph" in arg) {
			return new Symlink({ object: arg });
		}
		let artifact = arg.artifact;
		let subpath = arg.subpath !== undefined ? arg.subpath : undefined;
		let object = { artifact, subpath };
		return new Symlink({ object });
	}

	static async arg(...args: tg.Args<Symlink.Arg>): Promise<Symlink.ArgObject> {
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
		let flattened = flatten(resolved);
		let objects = await Promise.all(
			flattened.map(async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (typeof arg === "string") {
					return { subpath: arg };
				} else if (tg.Artifact.is(arg)) {
					return { artifact: arg, subpath: tg.Mutation.unset() };
				} else if (arg instanceof tg.Template) {
					tg.assert(arg.components.length <= 2);
					let [firstComponent, secondComponent] = arg.components;
					if (
						typeof firstComponent === "string" &&
						secondComponent === undefined
					) {
						return { subpath: firstComponent };
					} else if (
						tg.Artifact.is(firstComponent) &&
						secondComponent === undefined
					) {
						return { artifact: firstComponent, subpath: tg.Mutation.unset() };
					} else if (
						tg.Artifact.is(firstComponent) &&
						typeof secondComponent === "string"
					) {
						tg.assert(secondComponent.startsWith("/"));
						return {
							artifact: firstComponent,
							subpath: secondComponent.slice(1),
						};
					} else {
						throw new Error("invalid template");
					}
				} else if (arg instanceof Symlink) {
					let subpath = await arg.subpath();
					return { artifact: await arg.artifact(), subpath };
				} else {
					return arg;
				}
			}),
		);
		let mutations = await tg.Args.createMutations(objects, {
			artifact: "set",
			subpath: "set",
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
			let object = await syscall("object_load", this.#state.id!);
			tg.assert(object.kind === "symlink");
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

	async artifact(): Promise<tg.Artifact | undefined> {
		const object = await this.object();
		if (!("graph" in object)) {
			return object.artifact;
		} else {
			const nodes = await object.graph.nodes();
			const node = nodes[object.node];
			tg.assert(node !== undefined, `invalid index ${object.node}`);
			tg.assert(
				node.kind === "symlink",
				`expected a symlink node, got ${node}`,
			);
			const artifact = node.artifact;
			if (artifact === undefined || tg.Artifact.is(artifact)) {
				return artifact;
			} else {
				const node = nodes[artifact];
				tg.assert(node !== undefined, `invalid index ${artifact}`);
				switch (node.kind) {
					case "directory": {
						return new tg.Directory({
							object: { graph: object.graph, node: artifact },
						});
					}
					case "file": {
						return new tg.File({
							object: { graph: object.graph, node: artifact },
						});
					}
					case "symlink": {
						return new tg.Symlink({
							object: { graph: object.graph, node: artifact },
						});
					}
				}
			}
		}
	}

	async subpath(): Promise<string | undefined> {
		const object = await this.object();
		if (!("graph" in object)) {
			return object.subpath;
		} else {
			const nodes = await object.graph.nodes();
			const node = nodes[object.node];
			tg.assert(node !== undefined, `invalid index ${object.node}`);
			tg.assert(
				node.kind === "symlink",
				`expected a symlink node, got ${node}`,
			);
			return node.subpath;
		}
	}

	async resolve(
		from?: Symlink.Arg,
	): Promise<tg.Directory | tg.File | undefined> {
		from = from ? await symlink(from) : undefined;
		let fromArtifact = await from?.artifact();
		if (fromArtifact instanceof Symlink) {
			fromArtifact = await fromArtifact.resolve();
		}
		let fromPath = await from?.subpath();
		let artifact = await this.artifact();
		if (artifact instanceof Symlink) {
			artifact = await artifact.resolve();
		}
		let subpath = await this.subpath();
		if (artifact !== undefined && fromArtifact !== undefined) {
			throw new Error("expected no `from` value when `artifact` is set");
		}
		if (artifact !== undefined && !subpath) {
			return artifact;
		} else if (artifact === undefined && subpath && fromPath) {
			if (!(fromArtifact instanceof tg.Directory)) {
				throw new Error("expected a directory");
			}
			return await fromArtifact.tryGet(
				tg.path.join(tg.path.parent(fromPath) ?? "", subpath),
			);
		} else if (artifact !== undefined && subpath) {
			if (!(artifact instanceof tg.Directory)) {
				throw new Error("expected a directory");
			}
			return await artifact.tryGet(subpath);
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

	export type ArgObject =
		| {
				artifact?: tg.Artifact | undefined;
				subpath?: string | undefined;
		  }
		| { graph: tg.Graph; node: number };

	export type Id = string;

	export type Object =
		| {
				artifact: tg.Artifact | undefined;
				subpath: string | undefined;
		  }
		| { graph: tg.Graph; node: number };

	export type State = tg.Object.State<Symlink.Id, Symlink.Object>;
}
