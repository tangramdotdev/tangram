import * as tg from "./index.ts";

export type Object =
	| tg.Blob
	| tg.Directory
	| tg.File
	| tg.Symlink
	| tg.Graph
	| tg.Command
	| tg.Error;

export namespace Object {
	export namespace Get {
		export type Arg = {
			location?: tg.Location.Arg | null;
			metadata?: boolean;
			tokens?: tg.Authorization.Tokens | null;
		};

		export type Output = {
			data: tg.Object.Data;
			tokens?: tg.Authorization.Tokens | null;
		};
	}

	export namespace Batch {
		export type Object = {
			children?: Array<tg.Referent<tg.Object.Id>> | null;
			id: tg.Object.Id;
			data: tg.Object.Data;
		};

		export type Arg = {
			location?: tg.Location.Arg | null;
			objects: Array<tg.Object.Batch.Object>;
		};

		export type Output = {
			objects: Array<tg.Referent<tg.Object.Id>>;
		};
	}

	export namespace Put {
		export type Arg = {
			children?: Array<tg.Referent<tg.Object.Id>> | null;
			data: tg.Object.Data;
			location?: tg.Location.Arg | null;
		};

		export type Output = {
			object: tg.Referent<tg.Object.Id>;
		};
	}

	export type Kind =
		| "blob"
		| "directory"
		| "file"
		| "symlink"
		| "graph"
		| "command"
		| "error";

	export type Id =
		| tg.Blob.Id
		| tg.Directory.Id
		| tg.File.Id
		| tg.Symlink.Id
		| tg.Graph.Id
		| tg.Command.Id
		| tg.Error.Id;

	export namespace Id {
		export let kind = (id: tg.Object.Id): tg.Object.Kind => {
			let prefix = id.substring(0, 3);
			if (prefix === "blb") {
				return "blob";
			} else if (prefix === "dir") {
				return "directory";
			} else if (prefix === "fil") {
				return "file";
			} else if (prefix === "sym") {
				return "symlink";
			} else if (prefix === "gph") {
				return "graph";
			} else if (prefix === "cmd") {
				return "command";
			} else if (prefix === "err") {
				return "error";
			} else {
				throw new Error(`invalid object id: ${id}`);
			}
		};
	}

	export class State {
		#id: tg.Object.Id | null;
		#loadPromise: Promise<tg.Object.Object> | null;
		#location: tg.Location | null;
		#object: tg.Object.Object | null;
		#stored: boolean;
		#storePromise: Promise<void> | null;
		#tokens: tg.Authorization.Tokens;

		constructor(arg: tg.Object.State.ConstructorArg) {
			this.#id = arg.id ?? null;
			this.#loadPromise = null;
			this.#location =
				arg.location === undefined || arg.location === null
					? null
					: { ...arg.location };
			this.#object = arg.object ?? null;
			this.#stored = arg.stored;
			this.#storePromise = null;
			this.#tokens = { ...arg.tokens };
		}

		get id(): tg.Object.Id {
			if (this.#id !== null) {
				return this.#id;
			}
			let data = tg.Object.Data.withoutLocationAndTokens(
				tg.Object.Object.toData(this.#object!),
			);
			this.#id = tg.client.objectId(data);
			return this.#id;
		}

		set id(id: tg.Object.Id) {
			this.#id = id;
		}

		get location(): tg.Location | null {
			return this.#location === null ? null : { ...this.#location };
		}

		set location(location: tg.Location | null) {
			this.#location = location === null ? null : { ...location };
		}

		inheritLocation(location: tg.Location | null): void {
			if (this.#location === null) {
				this.#location = location === null ? null : { ...location };
			}
		}

		get object(): tg.Object.Object | null {
			return this.#object;
		}

		set object(object: tg.Object.Object | null) {
			this.#object = object;
		}

		get stored(): boolean {
			return this.#stored;
		}

		set stored(stored: boolean) {
			this.#stored = stored;
		}

		get storePromise(): Promise<void> | null {
			return this.#storePromise;
		}

		startStorePromise(promise: Promise<void>): void {
			if (this.#stored || this.#storePromise !== null) {
				throw new Error("the object state cannot start a store promise");
			}
			this.#storePromise = promise;
		}

		finishStore(object: tg.Referent<tg.Object.Id>): void {
			if (this.id !== object.node) {
				throw new Error("invalid object batch output");
			}
			this.location = object.options?.location ?? null;
			this.#stored = true;
			this.tokens = object.options?.tokens ?? {};
		}

		clearStorePromise(promise: Promise<void>): void {
			if (this.#storePromise === promise) {
				this.#storePromise = null;
			}
		}

		get tokens(): tg.Authorization.Tokens {
			return { ...this.#tokens };
		}

		set tokens(tokens: tg.Authorization.Tokens) {
			this.#tokens = { ...tokens };
		}

		inheritTokens(tokens: tg.Authorization.Tokens): void {
			tg.Authorization.Tokens.inherit(this.#tokens, tokens);
		}

		get kind(): tg.Object.Kind {
			if (this.#object !== null) {
				return this.#object.kind;
			}
			return tg.Object.Id.kind(this.#id!);
		}

		async load(): Promise<tg.Object.Object> {
			if (this.#object !== null) {
				return this.#object;
			}
			if (this.#loadPromise === null) {
				let promise = Promise.resolve().then(() => this.#load());
				this.#loadPromise = promise;
				promise.then(
					() => this.#clearLoadPromise(promise),
					() => this.#clearLoadPromise(promise),
				);
			}

			return await this.#loadPromise;
		}

		async #load(): Promise<tg.Object.Object> {
			let arg: tg.Object.Get.Arg = {
				location:
					this.#location === null
						? null
						: tg.Location.Arg.fromLocation(this.#location),
				tokens: { ...this.#tokens },
			};
			let output = await tg.client.getObject(this.#id!, arg);
			if (
				output.tokens !== undefined &&
				output.tokens !== null &&
				!tg.Authorization.Tokens.isEmpty(output.tokens)
			) {
				this.#tokens = { ...output.tokens };
			}
			this.#object = tg.Object.Object.fromData(output.data);

			return this.#object;
		}

		#clearLoadPromise(promise: Promise<tg.Object.Object>): void {
			if (this.#loadPromise === promise) {
				this.#loadPromise = null;
			}
		}

		unload(): void {
			if (this.#stored) {
				this.#object = null;
			}
		}

		get children(): Promise<Array<tg.Object>> {
			return (async () => {
				await this.load();
				let children = tg.Object.Object.children(this.#object!);

				for (let child of children) {
					child.state.inheritLocation(this.#location);
					tg.Object.inheritTokens(child, this.#tokens);
				}

				return children;
			})();
		}
	}

	export namespace State {
		export type ConstructorArg = {
			id?: tg.Object.Id | null;
			location?: tg.Location | null;
			object?: tg.Object.Object | null;
			stored: boolean;
			tokens?: tg.Authorization.Tokens | null;
		};
	}

	export type Object =
		| { kind: "blob"; value: tg.Blob.Object }
		| { kind: "directory"; value: tg.Directory.Object }
		| { kind: "file"; value: tg.File.Object }
		| { kind: "symlink"; value: tg.Symlink.Object }
		| { kind: "graph"; value: tg.Graph.Object }
		| { kind: "command"; value: tg.Command.Object }
		| { kind: "error"; value: tg.Error.Object };

	export namespace Object {
		export let toData = (object: tg.Object.Object): tg.Object.Data => {
			switch (object.kind) {
				case "blob": {
					let value = tg.Blob.Object.toData(object.value);
					return { kind: "blob", value };
				}
				case "directory": {
					let value = tg.Directory.Object.toData(object.value);
					return { kind: "directory", value };
				}
				case "file": {
					let value = tg.File.Object.toData(object.value);
					return { kind: "file", value };
				}
				case "symlink": {
					let value = tg.Symlink.Object.toData(object.value);
					return { kind: "symlink", value };
				}
				case "graph": {
					let value = tg.Graph.Object.toData(object.value);
					return { kind: "graph", value };
				}
				case "command": {
					let value = tg.Command.Object.toData(object.value);
					return { kind: "command", value };
				}
				case "error": {
					let value = tg.Error.Object.toData(object.value);
					return { kind: "error", value };
				}
			}
		};

		export let fromData = (data: tg.Object.Data): tg.Object.Object => {
			switch (data.kind) {
				case "blob": {
					let value = tg.Blob.Object.fromData(data.value);
					return { kind: "blob", value };
				}
				case "directory": {
					let value = tg.Directory.Object.fromData(data.value);
					return { kind: "directory", value };
				}
				case "file": {
					let value = tg.File.Object.fromData(data.value);
					return { kind: "file", value };
				}
				case "symlink": {
					let value = tg.Symlink.Object.fromData(data.value);
					return { kind: "symlink", value };
				}
				case "graph": {
					let value = tg.Graph.Object.fromData(data.value);
					return { kind: "graph", value };
				}
				case "command": {
					let value = tg.Command.Object.fromData(data.value);
					return { kind: "command", value };
				}
				case "error": {
					let value = tg.Error.Object.fromData(data.value);
					return { kind: "error", value };
				}
			}
		};

		export let children = (object: tg.Object.Object): Array<tg.Object> => {
			switch (object.kind) {
				case "blob": {
					return tg.Blob.Object.children(object.value);
				}
				case "directory": {
					return tg.Directory.Object.children(object.value);
				}
				case "file": {
					return tg.File.Object.children(object.value);
				}
				case "symlink": {
					return tg.Symlink.Object.children(object.value);
				}
				case "graph": {
					return tg.Graph.Object.children(object.value);
				}
				case "command": {
					return tg.Command.Object.children(object.value);
				}
				case "error": {
					return tg.Error.Object.children(object.value);
				}
			}
		};
	}

	export type Data =
		| { kind: "blob"; value: tg.Blob.Data }
		| { kind: "directory"; value: tg.Directory.Data }
		| { kind: "file"; value: tg.File.Data }
		| { kind: "symlink"; value: tg.Symlink.Data }
		| { kind: "graph"; value: tg.Graph.Data }
		| { kind: "command"; value: tg.Command.Data }
		| { kind: "error"; value: tg.Error.Data };

	export namespace Data {
		export let children = (data: tg.Object.Data): Array<tg.Object.Id> => {
			switch (data.kind) {
				case "blob": {
					return tg.Blob.Data.children(data.value);
				}
				case "directory": {
					return tg.Directory.Data.children(data.value);
				}
				case "file": {
					return tg.File.Data.children(data.value);
				}
				case "symlink": {
					return tg.Symlink.Data.children(data.value);
				}
				case "graph": {
					return tg.Graph.Data.children(data.value);
				}
				case "command": {
					return tg.Command.Data.children(data.value);
				}
				case "error": {
					return tg.Error.Data.children(data.value);
				}
			}
		};

		export let withoutLocationAndTokens = (
			data: tg.Object.Data,
		): tg.Object.Data => {
			switch (data.kind) {
				case "blob":
				case "directory":
				case "symlink": {
					return { ...data };
				}
				case "command": {
					return {
						...data,
						value: tg.Command.Data.withoutLocationAndTokens(data.value),
					};
				}
				case "error": {
					return {
						...data,
						value: tg.Error.Data.withoutLocationAndTokens(data.value),
					};
				}
				case "file": {
					return {
						...data,
						value: tg.File.Data.withoutLocationAndTokens(data.value),
					};
				}
				case "graph": {
					return {
						...data,
						value: tg.Graph.Data.withoutLocationAndTokens(data.value),
					};
				}
			}
		};
	}

	export let toReferent = <T extends tg.Object>(
		object: T,
	): tg.Referent<T["id"]> => {
		let options = {
			location: object.state.location,
			tokens: object.state.tokens,
		};
		return { node: object.id, options };
	};

	/** Get an object with a referent. */
	export let withReferent = (
		referent: tg.Referent<tg.Object.Id>,
	): tg.Object => {
		let object = withId(referent.node);
		object.state.location = referent.options?.location ?? null;
		object.state.tokens = referent.options?.tokens ?? {};
		return object;
	};

	/** Get an object with an ID. */
	export let withId = (id: tg.Object.Id): tg.Object => {
		let prefix = id.substring(0, 3);
		if (prefix === "blb") {
			return tg.Blob.withId(id);
		} else if (prefix === "dir") {
			return tg.Directory.withId(id);
		} else if (prefix === "fil") {
			return tg.File.withId(id);
		} else if (prefix === "sym") {
			return tg.Symlink.withId(id);
		} else if (prefix === "gph") {
			return tg.Graph.withId(id);
		} else if (prefix === "cmd") {
			return tg.Command.withId(id);
		} else if (prefix === "err") {
			return tg.Error.withId(id);
		} else {
			throw new Error(`invalid object id: ${id}`);
		}
	};

	/** Check if a value is a `tg.Object`. */
	export let is = (value: unknown): value is tg.Object => {
		return (
			value instanceof tg.Blob ||
			value instanceof tg.Directory ||
			value instanceof tg.File ||
			value instanceof tg.Symlink ||
			value instanceof tg.Graph ||
			value instanceof tg.Command ||
			value instanceof tg.Error
		);
	};

	export let inheritTokens = (
		object: tg.Object,
		tokens: tg.Authorization.Tokens,
	): void => {
		object.state.inheritTokens(tokens);
	};

	export let inheritLocation = (
		object: tg.Object,
		location: tg.Location | null,
	): void => {
		object.state.inheritLocation(location);
	};

	/** Expect that a value is a `tg.Object`. */
	export let expect = (value: unknown): tg.Object => {
		tg.assert(tg.Object.is(value));
		return value;
	};

	/** Assert that a value is a `tg.Object`. */
	export let assert = (value: unknown): asserts value is tg.Object => {
		tg.assert(tg.Object.is(value));
	};

	export let kind = (object: tg.Object): tg.Object.Kind => {
		if (object instanceof tg.Blob) {
			return "blob";
		} else if (object instanceof tg.Directory) {
			return "directory";
		} else if (object instanceof tg.File) {
			return "file";
		} else if (object instanceof tg.Symlink) {
			return "symlink";
		} else if (object instanceof tg.Graph) {
			return "graph";
		} else if (object instanceof tg.Command) {
			return "command";
		} else if (object instanceof tg.Error) {
			return "error";
		} else {
			return tg.unreachable();
		}
	};
}
