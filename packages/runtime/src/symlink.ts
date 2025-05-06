import { isGraphArg } from "./artifact.ts";
import * as tg from "./index.ts";

export let symlink = async (arg: Symlink.Arg): Promise<Symlink> => {
	return await Symlink.new(arg);
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

	static async new(arg: tg.Unresolved<Symlink.Arg>): Promise<Symlink> {
		let resolved = await Symlink.arg(arg);
		if (isGraphArg(resolved)) {
			return new Symlink({ object: resolved });
		} else if ("target" in resolved) {
			let symlink = new Symlink({ object: resolved });
			return symlink;
		} else if ("artifact" in resolved) {
			return new Symlink({
				object: {
					artifact: resolved.artifact,
					subpath: resolved.subpath,
				},
			});
		}
		return tg.unreachable("invalid symlink arguments");
	}

	static async arg(
		arg: tg.Unresolved<Symlink.Arg>,
	): Promise<Symlink.ArgObject> {
		let resolved = await tg.resolve(arg);
		if (typeof resolved === "object" && "graph" in resolved) {
			return resolved;
		}
		if (typeof resolved === "string") {
			return { target: resolved };
		} else if (tg.Artifact.is(resolved)) {
			return { artifact: resolved, subpath: undefined };
		} else if (resolved instanceof tg.Template) {
			tg.assert(resolved.components.length <= 2);
			let [firstComponent, secondComponent] = resolved.components;
			if (typeof firstComponent === "string" && secondComponent === undefined) {
				return { target: firstComponent };
			} else if (
				tg.Artifact.is(firstComponent) &&
				secondComponent === undefined
			) {
				return { artifact: firstComponent, subpath: undefined };
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
		} else if (resolved instanceof Symlink) {
			let target = await resolved.target();
			let artifact = await resolved.artifact();
			let subpath = await resolved.subpath();
			if (target !== undefined) {
				return { target };
			} else if (artifact !== undefined) {
				return { artifact, subpath };
			} else {
				tg.unreachable();
			}
		} else {
			return resolved;
		}
		return tg.unreachable();
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

	async target(): Promise<string | undefined> {
		const object = await this.object();
		if ("graph" in object) {
			const nodes = await object.graph.nodes();
			const node = nodes[object.node];
			tg.assert(node !== undefined, `invalid index ${object.node}`);
			tg.assert(
				node.kind === "symlink",
				`expected a symlink node, got ${node}`,
			);
			if (!("target" in node)) {
				return undefined;
			}
			return node.target;
		} else if ("target" in object) {
			return object.target;
		} else if ("artifact" in object) {
			return undefined;
		}
	}

	async artifact(): Promise<tg.Artifact | undefined> {
		const object = await this.object();
		if ("artifact" in object) {
			return object.artifact;
		} else if ("graph" in object) {
			const nodes = await object.graph.nodes();
			const node = nodes[object.node];
			tg.assert(node !== undefined, `invalid index ${object.node}`);
			tg.assert(
				node.kind === "symlink",
				`expected a symlink node, got ${node}`,
			);
			if (!("artifact" in node)) {
				return undefined;
			}
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
		if ("graph" in object) {
			const nodes = await object.graph.nodes();
			const node = nodes[object.node];
			tg.assert(node !== undefined, `invalid index ${object.node}`);
			tg.assert(
				node.kind === "symlink",
				`expected a symlink node, got ${node}`,
			);
			if (!("subpath" in node)) {
				return undefined;
			}
			return node.subpath;
		} else if ("target" in object) {
			return undefined;
		} else if ("artifact" in object) {
			return object.subpath;
		}
	}

	async resolve(): Promise<tg.Artifact | undefined> {
		let artifact = await this.artifact();
		if (artifact instanceof Symlink) {
			artifact = await artifact.resolve();
		}
		let subpath = await this.subpath();
		if (artifact === undefined && subpath) {
			throw new Error("cannot resolve a symlink with no artifact");
		} else if (artifact !== undefined && !subpath) {
			return artifact;
		} else if (artifact instanceof tg.Directory && subpath) {
			return await artifact.tryGet(subpath);
		} else {
			throw new Error("invalid symlink");
		}
	}
}

export namespace Symlink {
	export type Arg = string | tg.Artifact | tg.Template | Symlink | ArgObject;

	export type ArgObject =
		| { graph: tg.Graph; node: number }
		| { target: string }
		| {
				artifact: tg.Artifact;
				subpath?: string | undefined;
		  };

	export type Id = string;

	export type Object =
		| { graph: tg.Graph; node: number }
		| { target: string }
		| {
				artifact: tg.Artifact;
				subpath: string | undefined;
		  };

	export type State = tg.Object.State<Symlink.Id, Symlink.Object>;
}
