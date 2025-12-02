import type * as tg from "./index.ts";

export type MaybePromise<T> = T | Promise<T>;

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
				[K in keyof T]?: T[K] | tg.Mutation<T[K]>;
			}
		: never;

export type MaybeReferent<T> = T | tg.Referent<T>;

export type UnresolvedArgs<T extends Array<tg.Value>> = {
	[K in keyof T]: tg.Unresolved<T[K]>;
};

export type ResolvedArgs<T extends Array<tg.Unresolved<tg.Value>>> = {
	[K in keyof T]: tg.Resolved<T[K]>;
};

export type ReturnValue = tg.MaybePromise<void> | tg.Unresolved<tg.Value>;

export type ResolvedReturnValue<T extends tg.ReturnValue> =
	T extends tg.MaybePromise<void>
		? undefined
		: T extends tg.Unresolved<tg.Value>
			? tg.Resolved<T>
			: never;
