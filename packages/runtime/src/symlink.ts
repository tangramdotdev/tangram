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
		return new Symlink({ id, stored: true });
	}

	static withObject(object: Symlink.Object): Symlink {
		return new Symlink({ object, stored: false });
	}

	static fromData(data: Symlink.Data): Symlink {
		return Symlink.withObject(Symlink.Object.fromData(data));
	}

	static async new(arg_: tg.Unresolved<Symlink.Arg>): Promise<Symlink> {
		let arg = await Symlink.arg(arg_);
		if ("graph" in arg) {
			return Symlink.withObject(
				arg as {
					graph: tg.Graph;
					node: number;
				},
			);
		} else {
			return Symlink.withObject({
				artifact: arg.artifact,
				path: arg.path,
			});
		}
	}

	static async arg(
		arg: tg.Unresolved<Symlink.Arg>,
	): Promise<Symlink.ArgObject> {
		let resolved = await tg.resolve(arg);
		if (typeof resolved === "string") {
			return { path: resolved };
		} else if (tg.Artifact.is(resolved)) {
			return { artifact: resolved };
		} else if (resolved instanceof tg.Template) {
			tg.assert(resolved.components.length <= 2);
			let [firstComponent, secondComponent] = resolved.components;
			if (typeof firstComponent === "string" && secondComponent === undefined) {
				return { path: firstComponent };
			} else if (
				tg.Artifact.is(firstComponent) &&
				secondComponent === undefined
			) {
				return { artifact: firstComponent, path: undefined };
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
		} else if (resolved instanceof Symlink) {
			let artifact = await resolved.artifact();
			let path = await resolved.path();
			return { artifact, path };
		} else {
			return resolved;
		}
	}

	static expect(value: unknown): Symlink {
		tg.assert(value instanceof Symlink);
		return value;
	}

	static assert(value: unknown): asserts value is Symlink {
		tg.assert(value instanceof Symlink);
	}

	get id(): Symlink.Id {
		if (this.#state.id! !== undefined) {
			return this.#state.id;
		}
		let object = this.#state.object!;
		let data = Symlink.Object.toData(object);
		let id = syscall("object_id", { kind: "symlink", value: data });
		this.#state.id = id;
		return id;
	}

	async object(): Promise<Symlink.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load(): Promise<tg.Symlink.Object> {
		if (this.#state.object === undefined) {
			let data = await syscall("object_get", this.#state.id!);
			tg.assert(data.kind === "symlink");
			let object = Symlink.Object.fromData(data.value);
			this.#state.object = object;
		}
		return this.#state.object!;
	}

	async store(): Promise<tg.Symlink.Id> {
		await tg.Value.store(this);
		return this.id;
	}

	async children(): Promise<Array<tg.Object>> {
		let object = await this.load();
		return tg.Symlink.Object.children(object);
	}

	async artifact(): Promise<tg.Artifact | undefined> {
		const object = await this.object();
		if (!("graph" in object)) {
			if (typeof object.artifact === "object" && "node" in object.artifact) {
				tg.assert(object.artifact.graph !== undefined, "missing graph");
				return await object.artifact.graph.get(object.artifact.node);
			}
			return object.artifact;
		} else {
			const node = (await object.graph.nodes())[object.node];
			tg.assert(node !== undefined, "invalid graph index");
			tg.assert(node.kind === "symlink", "expected a symlink");
			if (typeof node.artifact === "object" && "node" in node.artifact) {
				return await (node.artifact.graph ?? object.graph).get(
					node.artifact.node,
				);
			}
			return node.artifact;
		}
	}

	async path(): Promise<string | undefined> {
		const object = await this.object();
		if ("graph" in object) {
			const nodes = await object.graph.nodes();
			const node = nodes[object.node];
			tg.assert(node !== undefined, `invalid index ${object.node}`);
			tg.assert(
				node.kind === "symlink",
				`expected a symlink node, got ${node}`,
			);
			return node.path;
		} else {
			return object.path;
		}
	}

	async resolve(): Promise<tg.Artifact | undefined> {
		let artifact = await this.artifact();
		if (artifact instanceof Symlink) {
			artifact = await artifact.resolve();
		}
		let path = await this.path();
		if (artifact === undefined && path !== undefined) {
			throw new Error("cannot resolve a symlink with no artifact");
		} else if (artifact !== undefined && path === undefined) {
			return artifact;
		} else if (artifact instanceof tg.Directory && path !== undefined) {
			return await artifact.tryGet(path);
		} else {
			throw new Error("invalid symlink");
		}
	}
}

export namespace Symlink {
	export type Arg = string | tg.Artifact | tg.Template | Symlink | ArgObject;

	export type ArgObject =
		| { graph: tg.Graph; node: number }
		| {
				artifact?: tg.Artifact | undefined;
				path?: string | undefined;
		  };

	export type Id = string;

	export type Object =
		| { graph: tg.Graph; node: number }
		| {
				artifact: tg.Graph.Object.Edge<tg.Artifact> | undefined;
				path: string | undefined;
		  };

	export namespace Object {
		export let toData = (object: Object): Data => {
			if ("graph" in object) {
				return {
					graph: object.graph.id,
					node: object.node,
				};
			} else {
				let output: Data = {};
				if (object.artifact !== undefined) {
					output.artifact =
						"node" in object.artifact
							? { graph: object.artifact.graph?.id, node: object.artifact.node }
							: object.artifact.id;
				}
				if (object.path !== undefined) {
					output.path = object.path;
				}
				return output;
			}
		};

		export let fromData = (data: Data): Object => {
			if ("graph" in data) {
				return {
					graph: tg.Graph.withId(data.graph),
					node: data.node,
				};
			} else {
				return {
					artifact:
						typeof data.artifact === "object" && "node" in data.artifact
							? {
									graph: data.artifact.graph
										? tg.Graph.withId(data.artifact.graph)
										: undefined,
									node: data.artifact.node,
								}
							: typeof data.artifact === "string"
								? tg.Artifact.withId(data.artifact)
								: undefined,
					path: data.path,
				};
			}
		};

		export let children = (object: Object): Array<tg.Object> => {
			if ("graph" in object) {
				return [object.graph];
			} else if ("artifact" in object && object.artifact !== undefined) {
				if ("node" in object.artifact) {
					if (object.artifact.graph !== undefined) {
						return [object.artifact.graph];
					}
				} else {
					return [object.artifact];
				}
			}
			return [];
		};
	}

	export type Data =
		| { graph: tg.Graph.Id; node: number }
		| {
				artifact?: tg.Graph.Data.Edge<tg.Artifact.Id>;
				path?: string;
		  };

	export type State = tg.Object.State<Symlink.Id, Symlink.Object>;
}
