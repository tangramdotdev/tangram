import * as tg from "./index.ts";

/** Create an error. */
export function error(): tg.Error;
export function error(message: string, arg?: tg.Error.Arg | null): tg.Error;
export function error(arg: tg.Error.Arg): tg.Error;
export function error(
	firstArg?: string | tg.Error.Arg,
	secondArg?: tg.Error.Arg | null,
): tg.Error {
	let object: tg.Error.Object = { values: {} };
	let stackProvided = false;
	if (firstArg != null && typeof firstArg === "string") {
		object.message = firstArg;
		if (secondArg != null) {
			if (secondArg.code !== undefined && secondArg.code !== null) {
				object.code = secondArg.code;
			}
			if (
				secondArg.diagnostics !== undefined &&
				secondArg.diagnostics !== null
			) {
				object.diagnostics = secondArg.diagnostics;
			}
			if (secondArg.location !== undefined && secondArg.location !== null) {
				object.location = secondArg.location;
			}
			if (secondArg.message !== undefined && secondArg.message !== null) {
				object.message = secondArg.message;
			}
			if (secondArg.source !== undefined && secondArg.source !== null) {
				object.source = secondArg.source;
			}
			if ("stack" in secondArg) {
				stackProvided = true;
				if (secondArg.stack !== undefined && secondArg.stack !== null) {
					object.stack = secondArg.stack;
				}
			}
			if ("values" in secondArg) {
				object.values = secondArg.values ?? {};
			}
		}
	} else if (firstArg != null && typeof firstArg === "object") {
		if (firstArg.code !== undefined && firstArg.code !== null) {
			object.code = firstArg.code;
		}
		if (firstArg.diagnostics !== undefined && firstArg.diagnostics !== null) {
			object.diagnostics = firstArg.diagnostics;
		}
		if (firstArg.location !== undefined && firstArg.location !== null) {
			object.location = firstArg.location;
		}
		if (firstArg.message !== undefined && firstArg.message !== null) {
			object.message = firstArg.message;
		}
		if (firstArg.source !== undefined && firstArg.source !== null) {
			object.source = firstArg.source;
		}
		if ("stack" in firstArg) {
			stackProvided = true;
			if (firstArg.stack !== undefined && firstArg.stack !== null) {
				object.stack = firstArg.stack;
			}
		}
		if ("values" in firstArg) {
			object.values = firstArg.values ?? {};
		}
	}
	if (!stackProvided) {
		// @ts-expect-error
		globalThis.Error.captureStackTrace(object, tg.error);
	}
	return tg.Error.withObject(object);
}

/** An error. */
export class Error {
	#state: tg.Object.State;

	constructor(arg: {
		id?: tg.Error.Id;
		object?: tg.Error.Object;
		stored: boolean;
	}) {
		let object =
			arg.object !== undefined
				? { kind: "error" as const, value: arg.object }
				: undefined;
		this.#state = new tg.Object.State({
			...(arg.id !== undefined ? { id: arg.id } : {}),
			...(object !== undefined ? { object } : {}),
			stored: arg.stored,
		});
	}

	get state(): tg.Object.State {
		return this.#state;
	}

	/** Get an error with an ID. */
	static withId(id: tg.Error.Id): tg.Error {
		return new tg.Error({ id, stored: true });
	}

	/** Get an error with an object. */
	static withObject(object: tg.Error.Object): tg.Error {
		return new tg.Error({ object, stored: false });
	}

	/** Create an error from data. */
	static fromData(data: tg.Error.Data): tg.Error {
		return tg.Error.withObject(tg.Error.Object.fromData(data));
	}

	/** Convert an error to data. */
	static toData(value: tg.Error): tg.Error.Data {
		let object = value.state.object;
		tg.assert(object?.kind === "error");
		return tg.Error.Object.toData(object.value);
	}

	static toDataOrId(
		value: tg.Error,
	): tg.Error.Data | tg.Grant.MaybeWithToken<tg.Error.Id> {
		if (value.state.stored) {
			let id = value.state.id as tg.Error.Id;
			let token = value.state.token;
			return token === undefined ? id : { id, token };
		}
		return tg.Error.toData(value);
	}

	/** Expect that a value is a `tg.Error`. */
	static expect(value: unknown): tg.Error {
		tg.assert(value instanceof tg.Error);
		return value;
	}

	/** Assert that a value is a `tg.Error`. */
	static assert(value: unknown): asserts value is tg.Error {
		tg.assert(value instanceof tg.Error);
	}

	/** Get this error's ID. */
	get id(): tg.Error.Id {
		let id = this.#state.id;
		tg.assert(tg.Object.Id.kind(id) === "error");
		return id;
	}

	/** Get this error's object. */
	async object(): Promise<tg.Error.Object> {
		let object = await this.#state.load();
		tg.assert(object.kind === "error");
		return object.value;
	}

	/** Load this error's object. */
	async load(): Promise<tg.Error.Object> {
		let object = await this.#state.load();
		tg.assert(object.kind === "error");
		return object.value;
	}

	/** Unload this error's object. */
	unload(): void {
		this.#state.unload();
	}

	/** Store this error. */
	async store(): Promise<tg.Error.Id> {
		await tg.Value.store(this);
		return this.id;
	}

	/** Get this error's children. */
	get children(): Promise<Array<tg.Object>> {
		return this.#state.children;
	}

	/** Get this error's code. */
	get code(): Promise<string | null> {
		return (async () => {
			let object = await this.object();
			return object.code ?? null;
		})();
	}

	/** Get this error's diagnostics. */
	get diagnostics(): Promise<Array<tg.Diagnostic> | null> {
		return (async () => {
			let object = await this.object();
			return object.diagnostics ?? null;
		})();
	}

	/** Get this error's location. */
	get location(): Promise<tg.Error.Location | null> {
		return (async () => {
			let object = await this.object();
			return object.location ?? null;
		})();
	}

	/** Get this error's message. */
	get message(): Promise<string | null> {
		return (async () => {
			let object = await this.object();
			return object.message ?? null;
		})();
	}

	/** Get this error's source. */
	get source(): Promise<tg.Referent<tg.Error> | null> {
		return (async () => {
			let object = await this.object();
			if (object.source === undefined) {
				return null;
			}
			if (object.source.item instanceof tg.Error) {
				return object.source as tg.Referent<tg.Error>;
			} else {
				return {
					...object.source,
					item: tg.Error.withObject(object.source.item),
				};
			}
		})();
	}

	/** Get this error's stack. */
	get stack(): Promise<Array<tg.Error.Location> | null> {
		return (async () => {
			let object = await this.object();
			return object.stack ?? null;
		})();
	}

	/** Get this error's values. */
	get values(): Promise<{ [key: string]: string }> {
		return (async () => {
			let object = await this.object();
			return object.values;
		})();
	}
}

export namespace Error {
	export type Id = string;

	export type Arg = {
		code?: string | null;
		diagnostics?: Array<tg.Diagnostic> | null;
		location?: tg.Error.Location | null;
		message?: string | null;
		source?: tg.Referent<tg.Error.Object | tg.Error> | null;
		stack?: Array<tg.Error.Location> | null;
		values?: { [key: string]: string } | null;
	};

	export type Object = {
		code?: string;
		diagnostics?: Array<tg.Diagnostic>;
		location?: tg.Error.Location;
		message?: string;
		source?: tg.Referent<tg.Error.Object | tg.Error>;
		stack?: Array<tg.Error.Location>;
		values: { [key: string]: string };
	};

	export namespace Object {
		export let toData = (object: tg.Error.Object): tg.Error.Data => {
			let data: tg.Error.Data = {};
			if (object.code !== undefined) {
				data.code = object.code;
			}
			if (object.diagnostics !== undefined) {
				data.diagnostics = object.diagnostics.map(tg.Diagnostic.toData);
			}
			if (object.location !== undefined) {
				data.location = tg.Error.Location.toData(object.location);
			}
			if (object.message !== undefined) {
				data.message = object.message;
			}
			if (object.source !== undefined) {
				data.source = tg.Referent.toData(object.source, (item) => {
					if (item instanceof tg.Error) {
						if (item.state.stored) {
							return item.id;
						} else {
							let obj = item.state.object;
							tg.assert(obj?.kind === "error");
							return tg.Error.Object.toData(obj.value);
						}
					} else {
						return tg.Error.Object.toData(item);
					}
				});
			}
			if (object.stack !== undefined) {
				data.stack = object.stack.map(tg.Error.Location.toData);
			}
			if (globalThis.Object.keys(object.values).length > 0) {
				data.values = object.values;
			}
			return data;
		};

		export let fromData = (data: tg.Error.Data): tg.Error.Object => {
			let object: tg.Error.Object = { values: data.values ?? {} };
			if (data.code !== undefined) {
				object.code = data.code;
			}
			if (data.diagnostics !== undefined) {
				object.diagnostics = data.diagnostics.map(tg.Diagnostic.fromData);
			}
			if (data.location !== undefined) {
				object.location = tg.Error.Location.fromData(data.location);
			}
			if (data.message !== undefined) {
				object.message = data.message;
			}
			if (data.source !== undefined) {
				object.source = tg.Referent.fromData(data.source, (item) => {
					if (typeof item === "string") {
						return tg.Error.withId(item);
					} else {
						return tg.Error.Object.fromData(item);
					}
				});
			}
			if (data.stack !== undefined) {
				object.stack = data.stack.map(tg.Error.Location.fromData);
			}
			return object;
		};

		export let children = (object: tg.Error.Object): Array<tg.Object> => {
			let diagnostics = (object.diagnostics ?? []).flatMap(
				tg.Diagnostic.children,
			);
			let location =
				object.location !== undefined
					? tg.Error.Location.children(object.location)
					: [];
			let stack = (object.stack ?? []).flatMap(tg.Error.Location.children);
			let source: Array<tg.Object>;
			if (object.source === undefined) {
				source = [];
			} else if (object.source.item instanceof tg.Error) {
				source = [object.source.item];
			} else {
				source = children(object.source.item);
			}
			return [...diagnostics, ...location, ...stack, ...source];
		};
	}

	export type Location = {
		symbol?: string;
		file: tg.Error.File;
		range: tg.Range;
	};

	export type File =
		| { kind: "internal"; value: string }
		| { kind: "module"; value: tg.Module };

	export type Data = {
		code?: string;
		diagnostics?: Array<tg.Diagnostic.Data>;
		location?: tg.Error.Data.Location;
		message?: string;
		source?: tg.Referent.Data<tg.Error.Data | tg.Error.Id>;
		stack?: Array<tg.Error.Data.Location>;
		values?: { [key: string]: string };
	};

	export namespace Data {
		export let children = (data: tg.Error.Data): Array<tg.Object.Id> => {
			let diagnostics = (data.diagnostics ?? []).flatMap(
				tg.Diagnostic.Data.children,
			);
			let location =
				data.location !== undefined
					? tg.Error.Data.Location.children(data.location)
					: [];
			let stack = (data.stack ?? []).flatMap(tg.Error.Data.Location.children);
			let source: Array<tg.Object.Id>;
			if (data.source === undefined) {
				source = [];
			} else if (typeof data.source === "string") {
				let [item] = data.source.split("?");
				source = item !== undefined && item !== "" ? [item] : [];
			} else if (typeof data.source.item === "string") {
				source = [data.source.item];
			} else {
				source = tg.Error.Data.children(data.source.item);
			}
			return [...diagnostics, ...location, ...stack, ...source];
		};

		export type Location = {
			symbol?: string;
			file: tg.Error.Data.File;
			range: tg.Range;
		};

		export type File =
			| { kind: "internal"; value: string }
			| { kind: "module"; value: tg.Module.Data };

		export namespace Location {
			export let children = (
				data: tg.Error.Data.Location,
			): Array<tg.Object.Id> => {
				return tg.Error.Data.File.children(data.file);
			};
		}

		export namespace File {
			export let children = (data: tg.Error.Data.File): Array<tg.Object.Id> => {
				if (data.kind === "module") {
					return tg.Module.Data.children(data.value);
				} else {
					return [];
				}
			};
		}
	}

	export namespace Location {
		export let toData = (value: tg.Error.Location): tg.Error.Data.Location => {
			let file =
				value.file.kind === "module"
					? {
							kind: "module" as const,
							value: tg.Module.toData(value.file.value),
						}
					: value.file;
			let data: tg.Error.Data.Location = { file, range: value.range };
			if (value.symbol !== undefined) {
				data.symbol = value.symbol;
			}
			return data;
		};

		export let fromData = (data: tg.Error.Data.Location): tg.Error.Location => {
			let file =
				data.file.kind === "module"
					? {
							kind: "module" as const,
							value: tg.Module.fromData(data.file.value),
						}
					: data.file;
			let location: tg.Error.Location = { file, range: data.range };
			if (data.symbol !== undefined) {
				location.symbol = data.symbol;
			}
			return location;
		};

		export let children = (value: tg.Error.Location): Array<tg.Object> => {
			return tg.Error.File.children(value.file);
		};
	}

	export namespace File {
		export let children = (value: tg.Error.File): Array<tg.Object> => {
			if (value.kind === "module") {
				return tg.Module.children(value.value);
			} else {
				return [];
			}
		};
	}
}
