import * as tg from "./index.ts";

export let symlink = async (arg: tg.Symlink.Arg): Promise<tg.Symlink> => {
	return await tg.Symlink.new(arg);
};

export class Symlink {
	#state: tg.Object.State;

	constructor(arg: {
		id?: tg.Symlink.Id;
		object?: tg.Symlink.Object;
		stored: boolean;
	}) {
		let object =
			arg.object !== undefined
				? { kind: "symlink" as const, value: arg.object }
				: undefined;
		this.#state = new tg.Object.State({
			id: arg.id,
			object,
			stored: arg.stored,
		});
	}

	get state(): tg.Object.State {
		return this.#state;
	}

	static withId(id: tg.Symlink.Id): tg.Symlink {
		return new tg.Symlink({ id, stored: true });
	}

	static withPointer(pointer: tg.Graph.Pointer): tg.Symlink {
		return new tg.Symlink({ object: pointer, stored: false });
	}

	static withObject(object: tg.Symlink.Object): tg.Symlink {
		return new tg.Symlink({ object, stored: false });
	}

	static fromData(data: tg.Symlink.Data): tg.Symlink {
		return tg.Symlink.withObject(tg.Symlink.Object.fromData(data));
	}

	static async new(arg_: tg.Unresolved<tg.Symlink.Arg>): Promise<tg.Symlink> {
		let arg = await tg.Symlink.arg(arg_);
		if (tg.Graph.Arg.Pointer.is(arg)) {
			return tg.Symlink.withObject(tg.Graph.Pointer.fromArg(arg));
		}
		return tg.Symlink.withObject({
			artifact: tg.Graph.Edge.fromArg(arg.artifact),
			path: arg.path,
		});
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
		} else if (resolved instanceof tg.Symlink) {
			let artifact = await resolved.artifact();
			let path = await resolved.path();
			return { artifact, path };
		} else {
			return resolved;
		}
	}

	static expect(value: unknown): tg.Symlink {
		tg.assert(value instanceof tg.Symlink);
		return value;
	}

	static assert(value: unknown): asserts value is tg.Symlink {
		tg.assert(value instanceof tg.Symlink);
	}

	get id(): tg.Symlink.Id {
		let id = this.#state.id;
		tg.assert(tg.Object.Id.kind(id) === "symlink");
		return id;
	}

	async object(): Promise<tg.Symlink.Object> {
		let object = await this.#state.load();
		tg.assert(object.kind === "symlink");
		return object.value;
	}

	async load(): Promise<tg.Symlink.Object> {
		let object = await this.#state.load();
		tg.assert(object.kind === "symlink");
		return object.value;
	}

	unload(): void {
		this.#state.unload();
	}

	async store(): Promise<tg.Symlink.Id> {
		await tg.Value.store(this);
		return this.id;
	}

	async children(): Promise<Array<tg.Object>> {
		return this.#state.children();
	}

	async artifact(): Promise<tg.Artifact | undefined> {
		let object = await this.object();
		if ("index" in object) {
			let graph = object.graph;
			tg.assert(graph !== undefined);
			let node = (await graph.nodes())[object.index];
			tg.assert(node !== undefined);
			tg.assert(node.kind === "symlink");
			if (typeof node.artifact === "number") {
				return await graph.get(node.artifact);
			} else if (
				typeof node.artifact === "object" &&
				"index" in node.artifact
			) {
				return await (node.artifact.graph ?? graph).get(node.artifact.index);
			}
			return node.artifact;
		} else {
			tg.assert(typeof object.artifact !== "number");
			if (typeof object.artifact === "object" && "index" in object.artifact) {
				tg.assert(object.artifact.graph !== undefined);
				return await object.artifact.graph.get(object.artifact.index);
			}
			return object.artifact;
		}
	}

	async path(): Promise<string | undefined> {
		let object = await this.object();
		if ("index" in object) {
			let graph = object.graph;
			tg.assert(graph !== undefined);
			let nodes = await graph.nodes();
			let node = nodes[object.index];
			tg.assert(node !== undefined);
			tg.assert(node.kind === "symlink");
			return node.path;
		} else {
			return object.path;
		}
	}

	async resolve(): Promise<tg.Artifact | undefined> {
		let artifact = await this.artifact();
		if (artifact instanceof tg.Symlink) {
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

	export type Arg =
		| string
		| tg.Artifact
		| tg.Template
		| Symlink
		| tg.Symlink.Arg.Object;

	export namespace Arg {
		export type Object = tg.Graph.Arg.Pointer | tg.Graph.Arg.Symlink;
	}

	export type Object = tg.Graph.Pointer | tg.Graph.Symlink;

	export namespace Object {
		export let toData = (object: tg.Symlink.Object): tg.Symlink.Data => {
			if ("index" in object) {
				return tg.Graph.Pointer.toData(object);
			} else {
				return tg.Graph.Symlink.toData(object);
			}
		};

		export let fromData = (data: tg.Symlink.Data): tg.Symlink.Object => {
			if (tg.Graph.Data.Pointer.is(data)) {
				return tg.Graph.Pointer.fromData(data);
			} else {
				return tg.Graph.Symlink.fromData(data);
			}
		};

		export let children = (object: tg.Symlink.Object): Array<tg.Object> => {
			if ("index" in object) {
				return tg.Graph.Pointer.children(object);
			} else {
				return tg.Graph.Symlink.children(object);
			}
		};
	}

	export type Data = tg.Graph.Data.Pointer | tg.Graph.Data.Symlink;
}
