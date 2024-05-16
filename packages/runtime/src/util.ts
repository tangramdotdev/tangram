import type { Blob } from "./blob.ts";
import type { Directory } from "./directory.ts";
import type { File } from "./file.ts";
import type { Lock } from "./lock.ts";
import type { Mutation } from "./mutation.ts";
import type { Symlink } from "./symlink.ts";
import type { Target } from "./target.ts";
import type { Template } from "./template.ts";
import type { Value } from "./value.ts";

export let flatten = <T>(value: MaybeNestedArray<T>): Array<T> => {
	if (value instanceof Array) {
		// @ts-ignore
		return value.flat(Number.POSITIVE_INFINITY);
	} else {
		return [value];
	}
};

export type MaybeMutation<T extends Value = Value> = T | Mutation<T>;

export type ValueOrMaybeMutationMap<T extends Value = Value> = T extends
	| undefined
	| boolean
	| number
	| string
	| Uint8Array
	| Blob
	| Directory
	| File
	| Symlink
	| Lock
	| Target
	| Mutation
	| Template
	| Array<infer _U extends Value>
	? T
	: T extends { [key: string]: Value }
		? MaybeMutationMap<T>
		: never;

export type MaybeMutationMap<
	T extends { [key: string]: Value } = { [key: string]: Value },
> = {
	[K in keyof T]?: MaybeMutation<T[K]>;
};

export type MutationMap<
	T extends { [key: string]: Value } = { [key: string]: Value },
> = {
	[K in keyof T]?: Mutation<T[K]>;
};

export type MaybeNestedArray<T> = T | Array<MaybeNestedArray<T>>;

export type MaybePromise<T> = T | Promise<T>;
