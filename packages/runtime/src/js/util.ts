import { Blob } from "./blob.ts";
import { Directory } from "./directory.ts";
import { File } from "./file.ts";
import { Lock } from "./lock.ts";
import { Mutation } from "./mutation.ts";
import { Symlink } from "./symlink.ts";
import { Target } from "./target.ts";
import { Template } from "./template.ts";
import { Value } from "./value.ts";

export let flatten = <T>(value: MaybeNestedArray<T>): Array<T> => {
	// @ts-ignore
	return value instanceof Array ? value.flat(Infinity) : [value];
};

export type MaybeMutation<T extends Value = Value> = T | Mutation<T>;

export type MaybeMutationMap<T extends Value = Value> = T extends
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
		? MutationMap<T>
		: never;

export type MaybeNestedArray<T> = T | Array<MaybeNestedArray<T>>;

export type MaybePromise<T> = T | Promise<T>;

export type MutationMap<
	T extends { [key: string]: Value } = { [key: string]: Value },
> = {
	[K in keyof T]?: MaybeMutation<T[K]>;
};
