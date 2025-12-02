import im from "immutable";
import * as tg from "./index.ts";

export type Unresolved<T extends tg.Value> = tg.MaybePromise<
	T extends
		| undefined
		| boolean
		| number
		| string
		| tg.Object
		| Uint8Array
		| tg.Mutation
		| tg.Template
		| tg.Placeholder
		? T
		: T extends Array<infer U extends tg.Value>
			? Array<tg.Unresolved<U>>
			: T extends { [key: string]: tg.Value }
				? { [K in keyof T]: tg.Unresolved<T[K]> }
				: never
>;

export type Resolved<T extends tg.Unresolved<tg.Value>> = T extends
	| undefined
	| boolean
	| number
	| string
	| tg.Object
	| Uint8Array
	| tg.Mutation
	| tg.Template
	| tg.Placeholder
	? T
	: T extends Array<infer U extends tg.Unresolved<tg.Value>>
		? Array<tg.Resolved<U>>
		: T extends { [key: string]: tg.Unresolved<tg.Value> }
			? { [K in keyof T]: tg.Resolved<T[K]> }
			: T extends Promise<infer U extends tg.Unresolved<tg.Value>>
				? tg.Resolved<U>
				: never;

export let resolve = async <T extends tg.Unresolved<tg.Value>>(
	value: T,
): Promise<tg.Resolved<T>> => {
	let inner = async <T extends tg.Unresolved<tg.Value>>(
		value: tg.Unresolved<tg.Value>,
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
			value instanceof tg.Blob ||
			value instanceof tg.Directory ||
			value instanceof tg.File ||
			value instanceof tg.Symlink ||
			value instanceof tg.Graph ||
			value instanceof tg.Command ||
			value instanceof Uint8Array ||
			value instanceof tg.Mutation ||
			value instanceof tg.Template ||
			value instanceof tg.Placeholder
		) {
			output = value as tg.Resolved<T>;
		} else if (value instanceof Array) {
			output = (await Promise.all(
				value.map((item) => inner(item, visited)),
			)) as tg.Resolved<T>;
		} else if (typeof value === "object") {
			output = Object.fromEntries(
				await Promise.all(
					Object.entries(value).map(async ([key, value]) => {
						value = await inner(value, visited);
						return [key, value];
					}),
				),
			) as tg.Resolved<T>;
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
