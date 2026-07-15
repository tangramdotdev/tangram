import im from "immutable";
import * as tg from "./index.ts";

/**
 * This computed type takes a type `T` and returns the union of all possible types that will return `T` by calling `resolve`. Here are some examples:
 *
 * ```
 * Unresolved<string> = MaybePromise<string>
 * Unresolved<{ key: string }> = MaybePromise<{ key: MaybePromise<string> }>
 * Unresolved<Array<{ key: string }>> = MaybePromise<Array<MaybePromise<{ key: MaybePromise<string> }>>>
 * ```
 */
export type Unresolved<T extends tg.Value> = tg.MaybePromise<
	T extends tg.Command<
		infer A extends Array<tg.Value>,
		infer O extends tg.Value
	>
		? UnresolvedCommand<A, O>
		: T extends
					| null
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

type UnresolvedCommand<A extends Array<tg.Value>, O extends tg.Value> =
	| tg.Command<A, O>
	| tg.Function<
			{
				[K in keyof A]: UnresolvedWithoutCommand<A[K]>;
			},
			UnresolvedWithoutCommand<O>
	  >;

type UnresolvedWithoutCommand<T extends tg.Value> = tg.MaybePromise<
	T extends
		| null
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
			? Array<UnresolvedWithoutCommand<U>>
			: T extends { [key: string]: tg.Value }
				? { [K in keyof T]: UnresolvedWithoutCommand<T[K]> }
				: never
>;

/**
 * This computed type performs the inverse of `tg.Unresolved`. It takes a type and returns the output of calling `tg.resolve` on a value of that type. Here are some examples:
 *
 * ```
 * Resolved<string> = string
 * Resolved<() => string> = string
 * Resolved<Promise<string>> = string
 * Resolved<Array<Promise<string>>> = Array<string>
 * Resolved<() => Promise<Array<Promise<string>>>> = Array<string>
 * Resolved<Promise<Array<Promise<string>>>> = Array<string>
 * ```
 */
export type Resolved<T extends tg.Unresolved<tg.Value>> =
	T extends PromiseLike<infer U extends tg.Unresolved<tg.Value>>
		? tg.Resolved<U>
		: T extends tg.Function<infer A, infer O>
			? tg.Command<tg.ResolvedArgs<A>, tg.ResolvedReturnValue<O>>
			: T extends
						| null
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
						: never;

/** Resolve all deeply nested promises in an unresolved value. */
export let resolve = async <T extends tg.Unresolved<tg.Value>>(
	value: T,
): Promise<tg.Resolved<T>> => {
	let inner = async <T extends tg.Unresolved<tg.Value>>(
		value: tg.Unresolved<tg.Value>,
		visited: im.Set<object>,
		path: string,
	): Promise<Resolved<T>> => {
		value = await value;
		let location = path === "" ? "" : ` at ${path}`;
		if (typeof value === "object" && value !== null) {
			if (visited.has(value)) {
				throw new Error(`cycle detected${location}`);
			}
			visited = visited.add(value);
		}
		let output: Resolved<T>;
		if (
			value === null ||
			typeof value === "boolean" ||
			typeof value === "number" ||
			typeof value === "string" ||
			value instanceof tg.Blob ||
			value instanceof tg.Directory ||
			value instanceof tg.File ||
			value instanceof tg.Symlink ||
			value instanceof tg.Graph ||
			value instanceof tg.Command ||
			value instanceof tg.Error ||
			value instanceof Uint8Array ||
			value instanceof tg.Mutation ||
			value instanceof tg.Template ||
			value instanceof tg.Placeholder
		) {
			output = value as tg.Resolved<T>;
		} else if (typeof value === "function") {
			output = (await tg.command(value)) as tg.Resolved<T>;
		} else if (value instanceof Array) {
			output = (await Promise.all(
				value.map((item, index) => inner(item, visited, `${path}[${index}]`)),
			)) as tg.Resolved<T>;
		} else if (typeof value === "object") {
			output = Object.fromEntries(
				await Promise.all(
					Object.entries(value).map(async ([key, value]) => [
						key,
						await inner(value, visited, `${path}.${key}`),
					]),
				),
			) as tg.Resolved<T>;
		} else {
			let type = typeof value;
			let description =
				type === "undefined"
					? "undefined is not a value, use null instead"
					: type;
			throw new Error(`invalid value to resolve${location}: ${description}`);
		}
		if (typeof value === "object" && value !== null) {
			visited = visited.delete(value);
		}
		return output;
	};
	return await inner(value, im.Set(), "");
};
