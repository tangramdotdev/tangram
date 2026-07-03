import type * as tg from "./index.ts";

export type MaybePromise<T> = T | Promise<T> | PromiseLike<T>;

export type MaybeMutation<T extends tg.Value = tg.Value> = T | tg.Mutation<T>;

export type MutationMap<
	T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
> = {
	[K in keyof T]?: tg.Mutation<T[K]>;
};

export type MaybeMutationMap<
	T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
> = {
	[K in keyof T]?: tg.MaybeMutation<T[K]>;
};

export type ValueOrMaybeMutationMap<T extends tg.Value = tg.Value> = T extends
	| undefined
	| boolean
	| number
	| string
	| tg.Object
	| Uint8Array
	| tg.Mutation
	| tg.Template
	| tg.Placeholder
	| Array<infer _U extends tg.Value>
	? T
	: T extends { [key: string]: tg.Value }
		? {
				[K in keyof T]?: tg.MaybeMutation<T[K]>;
			}
		: never;

export type MaybeReferent<T> = T | tg.Referent<T>;

export type UnresolvedArgs<T extends Array<tg.Value>> = {
	[K in keyof T]: tg.Unresolved<T[K]>;
};

export type ResolvedArgs<T extends Array<tg.Unresolved<tg.Value>>> = {
	[K in keyof T]: tg.Resolved<T[K]>;
};

export type Function<
	A extends tg.UnresolvedArgs<Array<tg.Value>>,
	O extends tg.ReturnValue,
> = (...args: A) => O;

export type ReturnValue<T extends tg.Value = tg.Value> =
	| (T extends undefined ? tg.MaybePromise<void> : never)
	| tg.Unresolved<T>;

export type ResolvedReturnValue<T extends tg.ReturnValue> =
	T extends tg.MaybePromise<void>
		? undefined
		: T extends tg.Unresolved<tg.Value>
			? tg.Resolved<T>
			: never;

export let undefinedToNull = (value: unknown): unknown => {
	if (value === undefined) {
		return null;
	} else if (Array.isArray(value)) {
		return value.map(undefinedToNull);
	} else if (isPlainObject(value)) {
		let output: { [key: string]: unknown } = {};
		for (let [key, entry] of Object.entries(value)) {
			output[key] = undefinedToNull(entry);
		}
		return output;
	} else {
		return value;
	}
};

export let nullToUndefined = <T>(value: unknown): T => {
	return replaceNull(value) as T;
};

let replaceNull = (value: unknown): unknown => {
	if (value === null) {
		return undefined;
	} else if (Array.isArray(value)) {
		return value.map(replaceNull);
	} else if (isPlainObject(value)) {
		let output: { [key: string]: unknown } = {};
		for (let [key, entry] of Object.entries(value)) {
			output[key] = replaceNull(entry);
		}
		return output;
	} else {
		return value;
	}
};

let isPlainObject = (value: unknown): value is { [key: string]: unknown } => {
	if (typeof value !== "object" || value === null) {
		return false;
	}
	let prototype = Object.getPrototypeOf(value);
	return prototype === Object.prototype || prototype === null;
};
