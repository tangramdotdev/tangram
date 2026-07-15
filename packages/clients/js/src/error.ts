import * as tg from "./index.ts";

/** Create an error. */
export function error(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<string>
): tg.Error.Builder;
export function error(...args: tg.Args<tg.Error.Arg>): tg.Error.Builder;
export function error(
	firstArg?:
		| TemplateStringsArray
		| tg.Unresolved<tg.ValueOrMaybeMutationMap<tg.Error.Arg>>,
	...args: tg.Args<tg.Error.Arg>
): tg.Error.Builder {
	return firstArg === undefined
		? new tg.Error.Builder(...args)
		: new tg.Error.Builder(firstArg, ...args);
}

export namespace error {
	export function sync(): tg.Error;
	export function sync(
		message: string,
		arg?: tg.Error.Arg.Object | null,
	): tg.Error;
	export function sync(arg: tg.Error.Arg.Object): tg.Error;
	export function sync(
		firstArg?: string | tg.Error.Arg.Object,
		secondArg?: tg.Error.Arg.Object | null,
	): tg.Error {
		let stackObject: { stack: Array<tg.Error.Location> | null } = {
			stack: null,
		};
		// @ts-expect-error
		globalThis.Error.captureStackTrace(stackObject, tg.error.sync);
		let object: tg.Error.Object = {
			code: null,
			diagnostics: null,
			location: null,
			message: null,
			source: null,
			stack: stackObject.stack,
			values: {},
		};
		let args: Array<tg.Error.Arg.Object | null | undefined> =
			typeof firstArg === "string"
				? [{ message: firstArg }, secondArg]
				: [firstArg];
		for (let arg of args) {
			if (arg === undefined || arg === null) {
				continue;
			}
			if (arg.code !== undefined) {
				object.code = arg.code;
			}
			if (arg.diagnostics !== undefined) {
				object.diagnostics = arg.diagnostics;
			}
			if (arg.location !== undefined) {
				object.location = arg.location;
			}
			if (arg.message !== undefined) {
				object.message = arg.message;
			}
			if (arg.source !== undefined) {
				object.source = arg.source;
			}
			if (arg.stack !== undefined) {
				object.stack = arg.stack;
			}
			if (arg.values !== undefined) {
				object.values = arg.values ?? {};
			}
		}
		return tg.Error.withObject(object);
	}
}

/** An error. */
export class Error {
	#state: tg.Object.State;

	constructor(arg: tg.Error.ConstructorArg) {
		let object =
			arg.object !== undefined
				? { kind: "error" as const, value: arg.object }
				: undefined;
		this.#state = new tg.Object.State({
			...(arg.id !== undefined ? { id: arg.id } : {}),
			...(object !== undefined ? { object } : {}),
			stored: arg.stored,
			...(arg.token !== undefined ? { token: arg.token } : {}),
		});
	}

	get state(): tg.Object.State {
		return this.#state;
	}

	/** Get an error with a referent. */
	static withReferent(referent: tg.Referent<tg.Error.Id>): tg.Error {
		let error = tg.Error.withId(referent.item);
		error.state.token = referent.options?.token ?? null;
		return error;
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

	/** Create an error. */
	static async new(...args: tg.Args<tg.Error.Arg>): Promise<tg.Error> {
		let stackObject: { stack: Array<tg.Error.Location> | null } = {
			stack: null,
		};
		// @ts-expect-error
		globalThis.Error.captureStackTrace(stackObject, tg.Error.new);
		let stack = stackObject.stack;
		let arg = await tg.Error.arg({ stack }, ...args);
		let object: tg.Error.Object = {
			code: arg.code ?? null,
			diagnostics: arg.diagnostics ?? null,
			location: arg.location ?? null,
			message: arg.message ?? null,
			source: arg.source ?? null,
			stack: arg.stack ?? null,
			values: arg.values ?? {},
		};
		return tg.Error.withObject(object);
	}

	static async arg(
		...args: tg.Args<tg.Error.Arg>
	): Promise<tg.Error.Arg.Object> {
		return await tg.Error.argResolved(
			...(await Promise.all(args.map(tg.resolve))),
		);
	}

	static async argResolved(
		...args: Array<tg.ValueOrMaybeMutationMap<tg.Error.Arg>>
	): Promise<tg.Error.Arg.Object> {
		return await tg.Args.applyResolved<tg.Error.Arg, tg.Error.Arg.Object>({
			args,
			map: async (arg) => {
				if (typeof arg === "string") {
					return { message: arg };
				} else if (arg instanceof tg.Error) {
					return await arg.object();
				} else {
					return arg;
				}
			},
			reduce: {
				code: "set",
				diagnostics: "set",
				location: "set",
				message: "set",
				source: "set",
				stack: "set",
				values: "merge",
			},
		});
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
			return token === null ? id : { id, token };
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
			if (object.source === null) {
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
	export type ConstructorArg = {
		id?: tg.Error.Id;
		object?: tg.Error.Object;
		stored: boolean;
		token?: tg.Grant.Token | null;
	};

	export class Builder {
		#args: tg.Args<tg.Error.Arg>;

		constructor(
			strings: TemplateStringsArray,
			...placeholders: tg.Args<string>
		);
		constructor(
			firstArg?:
				| TemplateStringsArray
				| tg.Unresolved<tg.ValueOrMaybeMutationMap<tg.Error.Arg>>,
			...args: tg.Args<tg.Error.Arg>
		);
		constructor(...args: tg.Args<tg.Error.Arg>);
		constructor(...args: any[]) {
			let firstArg = args[0];
			if (Array.isArray(firstArg) && "raw" in firstArg) {
				let strings = firstArg as TemplateStringsArray;
				let placeholders = args.slice(1) as tg.Args<string>;
				let components = [];
				for (let i = 0; i < strings.length - 1; i++) {
					let string = strings[i]!;
					components.push(string);
					let placeholder = placeholders[i]!;
					components.push(placeholder);
				}
				components.push(strings[strings.length - 1]!);
				let string = components.join("");
				this.#args = [string];
			} else {
				this.#args = args;
			}
			let stackObject: { stack: Array<tg.Error.Location> | null } = {
				stack: null,
			};
			// @ts-expect-error
			globalThis.Error.captureStackTrace(stackObject, tg.Error.Builder);
			let stack = stackObject.stack;
			this.#args.unshift({ stack });
		}

		code(code: tg.Unresolved<tg.MaybeMutation<string> | null>): this {
			this.#args.push({ code });
			return this;
		}

		diagnostics(
			diagnostics: tg.Unresolved<tg.MaybeMutation<Array<tg.Diagnostic>> | null>,
		): this {
			this.#args.push({ diagnostics });
			return this;
		}

		location(
			location: tg.Unresolved<tg.MaybeMutation<tg.Error.Location> | null>,
		): this {
			this.#args.push({ location });
			return this;
		}

		message(message: tg.Unresolved<tg.MaybeMutation<string> | null>): this {
			this.#args.push({ message });
			return this;
		}

		source(
			source: tg.Unresolved<tg.MaybeMutation<
				tg.Referent<tg.Error.Object | tg.Error>
			> | null>,
		): this {
			this.#args.push({ source });
			return this;
		}

		stack(
			stack: tg.Unresolved<tg.MaybeMutation<Array<tg.Error.Location>> | null>,
		): this {
			this.#args.push({ stack });
			return this;
		}

		values(
			values: tg.Unresolved<tg.MaybeMutation<{ [key: string]: string }> | null>,
		): this {
			this.#args.push({ values });
			return this;
		}

		then<TResult1 = tg.Error, TResult2 = never>(
			onfulfilled?:
				| ((value: tg.Error) => TResult1 | PromiseLike<TResult1>)
				| undefined
				| null,
			onrejected?:
				| ((reason: any) => TResult2 | PromiseLike<TResult2>)
				| undefined
				| null,
		): PromiseLike<TResult1 | TResult2> {
			return tg.Error.new(...this.#args).then(onfulfilled, onrejected);
		}
	}

	export type Arg = string | tg.Error | tg.Error.Arg.Object;

	export namespace Arg {
		export type Object = {
			code?: string | null;
			diagnostics?: Array<tg.Diagnostic> | null;
			location?: tg.Error.Location | null;
			message?: string | null;
			source?: tg.Referent<tg.Error.Object | tg.Error> | null;
			stack?: Array<tg.Error.Location> | null;
			values?: { [key: string]: string } | null;
		};
	}

	export type Object = {
		code: string | null;
		diagnostics: Array<tg.Diagnostic> | null;
		location: tg.Error.Location | null;
		message: string | null;
		source: tg.Referent<tg.Error.Object | tg.Error> | null;
		stack: Array<tg.Error.Location> | null;
		values: { [key: string]: string };
	};

	export namespace Object {
		export let toData = (object: tg.Error.Object): tg.Error.Data => {
			let data: tg.Error.Data = {};
			if (object.code !== null) {
				data.code = object.code;
			}
			if (object.diagnostics !== null) {
				data.diagnostics = object.diagnostics.map(tg.Diagnostic.toData);
			}
			if (object.location !== null) {
				data.location = tg.Error.Location.toData(object.location);
			}
			if (object.message !== null) {
				data.message = object.message;
			}
			if (object.source !== null) {
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
			if (object.stack !== null) {
				data.stack = object.stack.map(tg.Error.Location.toData);
			}
			if (globalThis.Object.keys(object.values).length > 0) {
				data.values = object.values;
			}
			return data;
		};

		export let fromData = (data: tg.Error.Data): tg.Error.Object => {
			let object: tg.Error.Object = {
				code: data.code ?? null,
				diagnostics:
					data.diagnostics !== undefined && data.diagnostics !== null
						? data.diagnostics.map(tg.Diagnostic.fromData)
						: null,
				location:
					data.location !== undefined && data.location !== null
						? tg.Error.Location.fromData(data.location)
						: null,
				message: data.message ?? null,
				source:
					data.source !== undefined && data.source !== null
						? tg.Referent.fromData(data.source, (item) => {
								if (typeof item === "string") {
									return tg.Error.withId(item);
								} else {
									return tg.Error.Object.fromData(item);
								}
							})
						: null,
				stack:
					data.stack !== undefined && data.stack !== null
						? data.stack.map(tg.Error.Location.fromData)
						: null,
				values: data.values ?? {},
			};
			return object;
		};

		export let children = (object: tg.Error.Object): Array<tg.Object> => {
			let diagnostics = (object.diagnostics ?? []).flatMap(
				tg.Diagnostic.children,
			);
			let location =
				object.location !== null
					? tg.Error.Location.children(object.location)
					: [];
			let stack = (object.stack ?? []).flatMap(tg.Error.Location.children);
			let source: Array<tg.Object>;
			if (object.source === null) {
				source = [];
			} else if (object.source.item instanceof tg.Error) {
				source = [object.source.item];
			} else {
				source = tg.Error.Object.children(object.source.item);
			}
			return [...diagnostics, ...location, ...stack, ...source];
		};
	}

	export type Location = {
		symbol: string | null;
		file: tg.Error.File;
		range: tg.Range;
	};

	export type File =
		| { kind: "internal"; value: string }
		| { kind: "module"; value: tg.Module };

	export type Data = {
		code?: string | null;
		diagnostics?: Array<tg.Diagnostic.Data> | null;
		location?: tg.Error.Data.Location | null;
		message?: string | null;
		source?: tg.Referent.Data<tg.Error.Data | tg.Error.Id> | null;
		stack?: Array<tg.Error.Data.Location> | null;
		values?: { [key: string]: string };
	};

	export namespace Data {
		export let children = (data: tg.Error.Data): Array<tg.Object.Id> => {
			let diagnostics = (data.diagnostics ?? []).flatMap(
				tg.Diagnostic.Data.children,
			);
			let location =
				data.location !== undefined && data.location !== null
					? tg.Error.Data.Location.children(data.location)
					: [];
			let stack = (data.stack ?? []).flatMap(tg.Error.Data.Location.children);
			let source: Array<tg.Object.Id>;
			if (data.source === undefined || data.source === null) {
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
			symbol?: string | null;
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
			if (value.symbol !== undefined && value.symbol !== null) {
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
			return {
				symbol: data.symbol ?? null,
				file,
				range: data.range,
			};
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
