import * as im from "immutable";
import * as tg from "./index.ts";

export namespace Resolve {
	export const atomic: unique symbol = Symbol();

	export type Atomic = { [atomic]: unknown };
}

/**
 * This computed type takes a type `T` and returns the union of all possible types that will return `T` by calling `resolve`. Here are some examples:
 *
 * ```
 * Unresolved<string> = MaybePromise<string>
 * Unresolved<{ key: string }> = MaybePromise<{ key: MaybePromise<string> }>
 * Unresolved<Array<{ key: string }>> = MaybePromise<Array<MaybePromise<{ key: MaybePromise<string> }>>>
 * ```
 */
export type Unresolved<T> = tg.MaybePromise<
	T extends tg.Command<
		infer A extends Array<tg.Value>,
		infer O extends tg.Value
	>
		? UnresolvedCommand<A, O>
		: T extends null | boolean | number | string | Uint8Array
			? T
			: typeof Resolve.atomic extends keyof T
				? T
				: T extends Array<infer U>
					? Array<tg.Unresolved<U>>
					: T extends object
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

type UnresolvedWithoutCommand<T> = tg.MaybePromise<
	T extends null | boolean | number | string | Uint8Array
		? T
		: typeof Resolve.atomic extends keyof T
			? T
			: T extends Array<infer U>
				? Array<UnresolvedWithoutCommand<U>>
				: T extends object
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
export type Resolved<T> =
	T extends PromiseLike<infer U>
		? tg.Resolved<U>
		: T extends tg.Function<infer A, infer O>
			? tg.Command<tg.ResolvedArgs<A>, tg.ResolvedReturnValue<O>>
			: T extends null | boolean | number | string | Uint8Array
				? T
				: typeof Resolve.atomic extends keyof T
					? T
					: T extends Array<infer U>
						? Array<tg.Resolved<U>>
						: T extends object
							? { [K in keyof T]: tg.Resolved<T[K]> }
							: never;

/** Resolve all deeply nested promises in an unresolved value. */
export let resolve = async <T>(value: T): Promise<tg.Resolved<T>> => {
	let inner = async <T>(
		value: T,
		visited: im.Set<object>,
		path: string,
	): Promise<Resolved<T>> => {
		let value_ = await value;
		let location = path === "" ? "" : ` at ${path}`;
		if (typeof value_ === "object" && value_ !== null) {
			if (visited.has(value_)) {
				throw new Error(`cycle detected${location}`);
			}
			visited = visited.add(value_);
		}
		let output: Resolved<T>;
		if (
			value_ === null ||
			typeof value_ === "boolean" ||
			typeof value_ === "number" ||
			typeof value_ === "string" ||
			value_ instanceof Uint8Array
		) {
			output = value_ as tg.Resolved<T>;
		} else if (
			typeof value_ === "object" &&
			value_ !== null &&
			tg.Resolve.atomic in value_
		) {
			output = value_ as tg.Resolved<T>;
		} else if (typeof value_ === "function") {
			output = (await tg.command(
				value_ as tg.Function<[], tg.ReturnValue>,
			)) as tg.Resolved<T>;
		} else if (value_ instanceof Array) {
			output = (await Promise.all(
				value_.map((node, index) => inner(node, visited, `${path}[${index}]`)),
			)) as tg.Resolved<T>;
		} else if (typeof value_ === "object") {
			output = Object.fromEntries(
				await Promise.all(
					Object.entries(value_).map(async ([key, value]) => [
						key,
						await inner(value, visited, `${path}.${key}`),
					]),
				),
			) as tg.Resolved<T>;
		} else {
			let type = typeof value_;
			let description =
				type === "undefined"
					? "undefined is not a value, use null instead"
					: type;
			throw new Error(`invalid value to resolve${location}: ${description}`);
		}
		if (typeof value_ === "object" && value_ !== null) {
			visited = visited.delete(value_);
		}
		return output;
	};
	return await inner(value, im.Set(), "");
};
