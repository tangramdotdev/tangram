import * as tg from "./index.ts";

export function error(): tg.Error;
export function error(message: string, arg?: tg.Error.Arg): tg.Error;
export function error(arg: tg.Error.Arg): tg.Error;
export function error(
	firstArg?: string | tg.Error.Arg,
	secondArg?: tg.Error.Arg,
): tg.Error {
	let object: tg.Error.Object = { values: {} };
	if (firstArg !== undefined && typeof firstArg === "string") {
		object.message = firstArg;
		if (secondArg !== undefined) {
			if ("code" in secondArg) {
				object.code = secondArg.code;
			}
			if ("diagnostics" in secondArg) {
				object.diagnostics = secondArg.diagnostics;
			}
			if ("location" in secondArg) {
				object.location = secondArg.location;
			}
			if ("message" in secondArg) {
				object.message = secondArg.message;
			}
			if ("source" in secondArg) {
				object.source = secondArg.source;
			}
			if ("stack" in secondArg) {
				object.stack = secondArg.stack;
			}
			if ("values" in secondArg) {
				object.values = secondArg.values ?? {};
			}
		}
	} else if (firstArg !== undefined && typeof firstArg === "object") {
		if ("code" in firstArg) {
			object.code = firstArg.code;
		}
		if ("diagnostics" in firstArg) {
			object.diagnostics = firstArg.diagnostics;
		}
		if ("location" in firstArg) {
			object.location = firstArg.location;
		}
		if ("message" in firstArg) {
			object.message = firstArg.message;
		}
		if ("source" in firstArg) {
			object.source = firstArg.source;
		}
		if ("stack" in firstArg) {
			object.stack = firstArg.stack;
		}
		if ("values" in firstArg) {
			object.values = firstArg.values ?? {};
		}
	}
	if (!("stack" in object)) {
		// @ts-expect-error
		globalThis.Error.captureStackTrace(object, tg.error);
	}
	return tg.Error.withObject(object);
}

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
			id: arg.id,
			object,
			stored: arg.stored,
		});
	}

	get state(): tg.Object.State {
		return this.#state;
	}

	static withId(id: tg.Error.Id): tg.Error {
		return new tg.Error({ id, stored: true });
	}

	static withObject(object: tg.Error.Object): tg.Error {
		return new tg.Error({ object, stored: false });
	}

	static fromData(data: tg.Error.Data): tg.Error {
		return tg.Error.withObject(tg.Error.Object.fromData(data));
	}

	static toData(value: tg.Error): tg.Error.Data {
		let object = value.state.object;
		tg.assert(object?.kind === "error");
		return tg.Error.Object.toData(object.value);
	}

	static expect(value: unknown): tg.Error {
		tg.assert(value instanceof tg.Error);
		return value;
	}

	static assert(value: unknown): asserts value is tg.Error {
		tg.assert(value instanceof tg.Error);
	}

	get id(): tg.Error.Id {
		let id = this.#state.id;
		tg.assert(tg.Object.Id.kind(id) === "error");
		return id;
	}

	async object(): Promise<tg.Error.Object> {
		let object = await this.#state.load();
		tg.assert(object.kind === "error");
		return object.value;
	}

	async load(): Promise<tg.Error.Object> {
		let object = await this.#state.load();
		tg.assert(object.kind === "error");
		return object.value;
	}

	unload(): void {
		this.#state.unload();
	}

	async store(): Promise<tg.Error.Id> {
		await tg.Value.store(this);
		return this.id;
	}

	get children(): Promise<Array<tg.Object>> {
		return this.#state.children();
	}

	get code(): Promise<string | undefined> {
		return (async () => {
			let object = await this.object();
			return object.code;
		})();
	}

	get diagnostics(): Promise<Array<tg.Diagnostic> | undefined> {
		return (async () => {
			let object = await this.object();
			return object.diagnostics;
		})();
	}

	get location(): Promise<tg.Error.Location | undefined> {
		return (async () => {
			let object = await this.object();
			return object.location;
		})();
	}

	get message(): Promise<string | undefined> {
		return (async () => {
			let object = await this.object();
			return object.message;
		})();
	}

	get source(): Promise<tg.Referent<tg.Error> | undefined> {
		return (async () => {
			let object = await this.object();
			if (object.source === undefined) {
				return undefined;
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

	get stack(): Promise<Array<tg.Error.Location> | undefined> {
		return (async () => {
			let object = await this.object();
			return object.stack;
		})();
	}

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
		code?: string | undefined;
		diagnostics?: Array<tg.Diagnostic> | undefined;
		location?: tg.Error.Location | undefined;
		message?: string;
		source?: tg.Referent<tg.Error.Object | tg.Error> | undefined;
		stack?: Array<tg.Error.Location> | undefined;
		values?: { [key: string]: string } | undefined;
	};

	export type Object = {
		code?: string | undefined;
		diagnostics?: Array<tg.Diagnostic> | undefined;
		location?: tg.Error.Location | undefined;
		message?: string | undefined;
		source?: tg.Referent<tg.Error.Object | tg.Error> | undefined;
		stack?: Array<tg.Error.Location> | undefined;
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
			let object: tg.Error.Object = { values: {} };
			if ("code" in data) {
				object.code = data.code;
			}
			if ("diagnostics" in data) {
				object.diagnostics = data.diagnostics?.map(tg.Diagnostic.fromData);
			}
			if ("location" in data) {
				object.location = tg.Error.Location.fromData(data.location);
			}
			if ("message" in data) {
				object.message = data.message;
			}
			if ("source" in data) {
				object.source = tg.Referent.fromData(data.source, (item) => {
					if (typeof item === "string") {
						return tg.Error.withId(item);
					} else {
						return tg.Error.Object.fromData(item);
					}
				});
			}
			if ("stack" in data) {
				object.stack = data.stack?.map(tg.Error.Location.fromData);
			}
			if ("values" in data) {
				object.values = data.values ?? {};
			}
			return object;
		};

		export let children = (object: tg.Error.Object): Array<tg.Object> => {
			let children: Array<tg.Object> = [];
			if (
				object.source !== undefined &&
				object.source.item instanceof tg.Error
			) {
				children.push(object.source.item);
			}
			return children;
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
		export type Location = {
			symbol?: string;
			file: tg.Error.Data.File;
			range: tg.Range;
		};

		export type File =
			| { kind: "internal"; value: string }
			| { kind: "module"; value: tg.Module.Data };
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
			return {
				...value,
				file: file,
			};
		};

		export let fromData = (data: tg.Error.Data.Location): tg.Error.Location => {
			let file =
				data.file.kind === "module"
					? {
							kind: "module" as const,
							value: tg.Module.fromData(data.file.value),
						}
					: data.file;
			return {
				...data,
				file,
			};
		};
	}
}
