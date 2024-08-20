import * as tg from "./index.ts";
import type { MaybePromise } from "./util.ts";

export type Unresolved<T extends tg.Value> = MaybePromise<
	T extends
		| undefined
		| boolean
		| number
		| string
		| tg.Object
		| Uint8Array
		| tg.Path
		| tg.Mutation
		| tg.Template
		? T
		: T extends Array<infer U extends tg.Value>
			? Array<Unresolved<U>>
			: T extends { [key: string]: tg.Value }
				? { [K in keyof T]: Unresolved<T[K]> }
				: never
>;

export type Resolved<T extends Unresolved<tg.Value>> = T extends
	| undefined
	| boolean
	| number
	| string
	| tg.Object
	| Uint8Array
	| tg.Path
	| tg.Mutation
	| tg.Template
	? T
	: T extends Promise<infer U extends Unresolved<tg.Value>>
		? Resolved<U>
		: T extends Array<infer U extends Unresolved<tg.Value>>
			? Array<Resolved<U>>
			: T extends { [key: string]: Unresolved<tg.Value> }
				? { [K in keyof T]: Resolved<T[K]> }
				: never;

export let resolve = async <T extends Unresolved<tg.Value>>(
	value: T,
): Promise<Resolved<T>> => {
	value = await value;
	if (
		value === undefined ||
		typeof value === "boolean" ||
		typeof value === "number" ||
		typeof value === "string" ||
		value instanceof tg.Leaf ||
		value instanceof tg.Branch ||
		value instanceof tg.Directory ||
		value instanceof tg.File ||
		value instanceof tg.Symlink ||
		value instanceof tg.Graph ||
		value instanceof tg.Target ||
		value instanceof Uint8Array ||
		value instanceof tg.Path ||
		value instanceof tg.Mutation ||
		value instanceof tg.Template
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
		throw new Error("invalid value to resolve");
	}
};
