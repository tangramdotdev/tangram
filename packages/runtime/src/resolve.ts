import * as im from "immutable";
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
	let inner = async <T extends Unresolved<tg.Value>>(
		value: Unresolved<tg.Value>,
		visited: im.Set<object>,
	): Promise<Resolved<T>> => {
		value = await value;
		if (typeof value === "object") {
			if (visited.has(value)) {
				throw new Error("cycle detected");
			}
			visited = visited.add(value);
		}
		let output: Resolved<T>;
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
			value instanceof tg.Mutation ||
			value instanceof tg.Template
		) {
			output = value as Resolved<T>;
		} else if (value instanceof Array) {
			output = (await Promise.all(
				value.map((item) => inner(item, visited)),
			)) as Resolved<T>;
		} else if (typeof value === "object") {
			output = Object.fromEntries(
				await Promise.all(
					Object.entries(value).map(async ([key, value]) => {
						value = await inner(value, visited);
						return [key, value];
					}),
				),
			) as Resolved<T>;
		} else {
			throw new Error("invalid value to resolve");
		}
		if (typeof value === "object") {
			visited = visited.delete(value);
		}
		return output;
	};
	return await inner(value, im.Set());
};
