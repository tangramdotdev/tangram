import * as tg from "./index.ts";

export let symlink = async (arg: tg.Symlink.Arg): Promise<tg.Symlink> => {
	return await tg.Symlink.new(arg);
};

export class Symlink {
	#state: tg.Symlink.State;

	constructor(state: tg.Symlink.State) {
		this.#state = state;
	}

	get state(): tg.Symlink.State {
		return this.#state;
	}

	static withId(id: tg.Symlink.Id): tg.Symlink {
		return new Symlink({ id, stored: true });
	}

	static withObject(object: tg.Symlink.Object): tg.Symlink {
		return new Symlink({ object, stored: false });
	}

	static fromData(data: tg.Symlink.Data): tg.Symlink {
		return tg.Symlink.withObject(tg.Symlink.Object.fromData(data));
	}

	static async new(arg_: tg.Unresolved<tg.Symlink.Arg>): Promise<tg.Symlink> {
		let arg = await tg.Symlink.arg(arg_);
		if (tg.Graph.Arg.Reference.is(arg)) {
			return tg.Symlink.withObject(tg.Graph.Reference.fromArg(arg));
		} else {
			return tg.Symlink.withObject({
				artifact: tg.Graph.Edge.fromArg(arg.artifact),
				path: arg.path,
			});
		}
	}

	static async arg(
		arg: tg.Unresolved<tg.Symlink.Arg>,
	): Promise<tg.Symlink.Arg.Object> {
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

	static expect(value: unknown): tg.Symlink {
		tg.assert(value instanceof Symlink);
		return value;
	}

	static assert(value: unknown): asserts value is tg.Symlink {
		tg.assert(value instanceof Symlink);
	}

	get id(): tg.Symlink.Id {
		if (this.#state.id! !== undefined) {
			return this.#state.id;
		}
		let object = this.#state.object!;
		let data = Symlink.Object.toData(object);
		let id = tg.handle.objectId({ kind: "symlink", value: data });
		this.#state.id = id;
		return id;
	}

	async object(): Promise<tg.Symlink.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load(): Promise<tg.Symlink.Object> {
		if (this.#state.object === undefined) {
			let data = await tg.handle.getObject(this.#state.id!);
			tg.assert(data.kind === "symlink");
			let object = tg.Symlink.Object.fromData(data.value);
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
		let object = await this.object();
		if ("node" in object) {
			let graph = object.graph;
			tg.assert(graph !== undefined);
			let node = (await graph.nodes())[object.node];
			tg.assert(node !== undefined);
			tg.assert(node.kind === "symlink");
			if (typeof node.artifact === "number") {
				return await graph.get(node.artifact);
			} else if (typeof node.artifact === "object" && "node" in node.artifact) {
				return await (node.artifact.graph ?? graph).get(node.artifact.node);
			}
			return node.artifact;
		} else {
			tg.assert(typeof object.artifact !== "number");
			if (typeof object.artifact === "object" && "node" in object.artifact) {
				tg.assert(object.artifact.graph !== undefined);
				return await object.artifact.graph.get(object.artifact.node);
			}
			return object.artifact;
		}
	}

	async path(): Promise<string | undefined> {
		let object = await this.object();
		if ("node" in object) {
			let graph = object.graph;
			tg.assert(graph !== undefined);
			let nodes = await graph.nodes();
			let node = nodes[object.node];
			tg.assert(node !== undefined);
			tg.assert(node.kind === "symlink");
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
	export type Id = string;

	export type State = tg.Object.State<Symlink.Id, Symlink.Object>;

	export type Arg =
		| string
		| tg.Artifact
		| tg.Template
		| Symlink
		| tg.Symlink.Arg.Object;

	export namespace Arg {
		export type Object = tg.Graph.Arg.Reference | tg.Graph.Arg.Symlink;
	}

	export type Object = tg.Graph.Reference | tg.Graph.Symlink;

	export namespace Object {
		export let toData = (object: tg.Symlink.Object): tg.Symlink.Data => {
			if ("node" in object) {
				return tg.Graph.Reference.toData(object);
			} else {
				return tg.Graph.Symlink.toData(object);
			}
		};

		export let fromData = (data: tg.Symlink.Data): tg.Symlink.Object => {
			if (tg.Graph.Data.Reference.is(data)) {
				return tg.Graph.Reference.fromData(data);
			} else {
				return tg.Graph.Symlink.fromData(data);
			}
		};

		export let children = (object: tg.Symlink.Object): Array<tg.Object> => {
			if ("node" in object) {
				return tg.Graph.Reference.children(object);
			} else {
				return tg.Graph.Symlink.children(object);
			}
		};
	}

	export type Data = tg.Graph.Data.Reference | tg.Graph.Data.Symlink;
}
