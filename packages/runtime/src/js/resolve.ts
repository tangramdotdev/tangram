import { Branch } from "./branch.ts";
import { Directory } from "./directory.ts";
import { File } from "./file.ts";
import { Leaf } from "./leaf.ts";
import { Lock } from "./lock.ts";
import { Mutation } from "./mutation.ts";
import { Symlink } from "./symlink.ts";
import { Target } from "./target.ts";
import { Template } from "./template.ts";
import { MaybePromise } from "./util.ts";
import { Value } from "./value.ts";

export type Unresolved<T extends Value> = MaybePromise<
	T extends
		| undefined
		| boolean
		| number
		| string
		| Uint8Array
		| Mutation
		| Template
		| Leaf
		| Branch
		| Directory
		| File
		| Symlink
		| Lock
		| Target
		? T
		: T extends Array<infer U extends Value>
			? Array<Unresolved<U>>
			: T extends { [key: string]: Value }
				? { [K in keyof T]: Unresolved<T[K]> }
				: never
>;

export type Resolved<T extends Unresolved<Value>> = T extends
	| undefined
	| boolean
	| number
	| string
	| Uint8Array
	| Mutation
	| Template
	| Leaf
	| Branch
	| Directory
	| File
	| Symlink
	| Lock
	| Target
	? T
	: T extends Promise<infer U extends Unresolved<Value>>
		? Resolved<U>
		: T extends Array<infer U extends Unresolved<Value>>
			? Array<Resolved<U>>
			: T extends { [key: string]: Unresolved<Value> }
				? { [K in keyof T]: Resolved<T[K]> }
				: never;

export let resolve = async <T extends Unresolved<Value>>(
	value: T,
): Promise<Resolved<T>> => {
	value = await value;
	if (
		value === undefined ||
		typeof value === "boolean" ||
		typeof value === "number" ||
		typeof value === "string" ||
		value instanceof Uint8Array ||
		value instanceof Template ||
		value instanceof Mutation ||
		value instanceof Leaf ||
		value instanceof Branch ||
		value instanceof Directory ||
		value instanceof File ||
		value instanceof Symlink ||
		value instanceof Lock ||
		value instanceof Target
	) {
		return value as unknown as Resolved<T>;
	} else if (value instanceof Array) {
		return (await Promise.all(
			value.map((value) => resolve(value)),
		)) as Resolved<T>;
	} else if (typeof value === "object") {
		return Object.fromEntries(
			await Promise.all(
				Object.entries(value).map(async ([key, value]) => [
					key,
					await resolve(value),
				]),
			),
		) as Resolved<T>;
	} else {
		throw new Error("Invalid value to resolve.");
	}
};
