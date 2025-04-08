import type * as tg from "./index.ts";
import { mutate } from "./mutation.ts";

export let flatten = <T>(value: MaybeNestedArray<T>): Array<T> => {
	if (value instanceof Array) {
		// @ts-ignore
		return value.flat(Number.POSITIVE_INFINITY);
	} else {
		return [value];
	}
};

export type MaybeNestedArray<T> = T | Array<MaybeNestedArray<T>>;

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
	[K in keyof T]?: MaybeMutation<T[K]>;
};

export type ValueOrMaybeMutationMap<T extends tg.Value = tg.Value> = T extends
	| undefined
	| boolean
	| number
	| string
	| Object
	| Uint8Array
	| tg.Mutation
	| tg.Template
	| Array<infer _U extends tg.Value>
	? T
	: T extends { [key: string]: tg.Value }
		? MaybeMutationMap<T>
		: never;

export let mergeMaybeMutationMaps = async <
	T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
>(
	maps: Array<tg.MaybeMutationMap<T>>,
): Promise<T> => {
	return await maps.reduce(
		async (object, mutations) => {
			if (mutations === undefined) {
				return Promise.resolve({}) as Promise<T>;
			}
			for (let [key, mutation] of Object.entries(mutations)) {
				await mutate(await object, key, mutation);
			}
			return object;
		},
		Promise.resolve({}) as Promise<T>,
	);
};
