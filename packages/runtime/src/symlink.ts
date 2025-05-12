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
		} else if ("target" in arg) {
			let symlink = Symlink.withObject(arg);
			return symlink;
		} else if ("artifact" in arg) {
			return Symlink.withObject({
				artifact: arg.artifact,
				subpath: arg.subpath,
			});
		}
		return tg.unreachable("invalid symlink arguments");
	}

	static async arg(
		arg: tg.Unresolved<Symlink.Arg>,
	): Promise<Symlink.ArgObject> {
		let resolved = await tg.resolve(arg);
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
		if (!("graph" in object)) {
			if ("artifact" in object) {
				return object.artifact;
			} else {
				return undefined;
			}
		} else {
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
						return tg.Directory.withObject({
							graph: object.graph,
							node: artifact,
						});
					}
					case "file": {
						return tg.File.withObject({ graph: object.graph, node: artifact });
					}
					case "symlink": {
						return tg.Symlink.withObject({
							graph: object.graph,
							node: artifact,
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
		| { target: string }
		| {
				artifact: tg.Artifact;
				subpath: string | undefined;
		  }
		| { graph: tg.Graph; node: number };

	export namespace Object {
		export let toData = (object: Object): Data => {
			if ("graph" in object) {
				return {
					graph: object.graph.id,
					node: object.node,
				};
			} else if ("target" in object) {
				return {
					target: object.target,
				};
			} else {
				let output: Data = {
					artifact: object.artifact.id,
				};
				if (object.subpath !== undefined) {
					output.subpath = object.subpath;
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
			} else if ("target" in data) {
				return {
					target: data.target,
				};
			} else {
				return {
					artifact: tg.Artifact.withId(data.artifact),
					subpath: data.subpath,
				};
			}
		};

		export let children = (object: Object): Array<tg.Object> => {
			if ("graph" in object) {
				return [object.graph];
			} else if ("artifact" in object) {
				return [object.artifact];
			} else {
				return [];
			}
		};
	}

	export type Data =
		| { target: string }
		| {
				artifact: tg.Artifact.Id;
				subpath?: string;
		  }
		| { graph: tg.Graph.Id; node: number };

	export type State = tg.Object.State<Symlink.Id, Symlink.Object>;
}
