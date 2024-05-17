import { Artifact } from "./artifact.ts";
import { assert as assert_, unreachable } from "./assert.ts";
import { Mutation } from "./mutation.ts";
import type { Unresolved } from "./resolve.ts";
import { Template, template } from "./template.ts";
import {
	type MaybeMutation,
	type MaybeMutationMap,
	type MaybeNestedArray,
	type MaybePromise,
	type MutationMap,
	type ValueOrMaybeMutationMap,
	flatten,
} from "./util.ts";
import type { Value } from "./value.ts";

export type Args<T extends Value = Value> = Array<
	Unresolved<MaybeNestedArray<ValueOrMaybeMutationMap<T>>>
>;

export namespace Args {
	export type Rules<
		T extends { [key: string]: Value } = { [key: string]: Value },
	> = {
		[K in keyof T]:
			| Mutation.Kind
			| ((arg: T[K]) => MaybePromise<Mutation<T[K]>>);
	};

	export let createMutations = async <
		T extends { [key: string]: Value } = { [key: string]: Value },
		R extends { [key: string]: Value } = T,
	>(
		args: Array<MaybeMutationMap<T>>,
		rules?: Rules<T>,
	): Promise<Array<MutationMap<R>>> => {
		return await Promise.all(
			args.map(async (arg) => {
				let object: { [key: string]: Mutation<T> } = {};
				for (let [key, value] of Object.entries(arg)) {
					if (value instanceof Mutation) {
						object[key] = value;
						continue;
					}
					let mutation = rules !== undefined ? rules[key] : undefined;
					if (mutation === undefined) {
						object[key] = await Mutation.set<typeof value>(value);
					} else if (typeof mutation === "string") {
						switch (mutation) {
							case "set":
								object[key] = await Mutation.set(value);
								break;
							case "unset":
								object[key] = Mutation.unset();
								break;
							case "set_if_unset":
								object[key] = await Mutation.setIfUnset(value);
								break;
							case "prepend":
								object[key] = await Mutation.prepend(value);
								break;
							case "append":
								object[key] = await Mutation.append(value);
								break;
							case "prefix":
								assert_(
									value instanceof Template ||
										Artifact.is(value) ||
										typeof value === "string",
								);
								object[key] = await Mutation.prefix(value);
								break;
							case "suffix":
								assert_(
									value instanceof Template ||
										Artifact.is(value) ||
										typeof value === "string",
								);
								object[key] = await Mutation.suffix(value);
								break;
							default:
								return unreachable(`unknown mutation kind "${mutation}"`);
						}
					} else {
						object[key] = await mutation(value);
					}
				}
				return object as MutationMap<T>;
			}),
		);
	};

	export let applyMutations = async <
		T extends { [key: string]: Value } = { [key: string]: Value },
	>(
		mutations: Array<MaybeMutationMap<T>>,
	): Promise<T> => {
		return await mutations.reduce(
			async (object, mutations) => {
				for (let [key, mutation] of Object.entries(mutations)) {
					await mutate(await object, key, mutation);
				}
				return object;
			},
			Promise.resolve({}) as Promise<T>,
		);
	};
}

let mutate = async (
	object: { [key: string]: Value },
	key: string,
	mutation: MaybeMutation,
) => {
	if (!(mutation instanceof Mutation)) {
		object[key] = mutation;
	} else if (mutation.inner.kind === "unset") {
		delete object[key];
	} else if (mutation.inner.kind === "set") {
		object[key] = mutation.inner.value;
	} else if (mutation.inner.kind === "set_if_unset") {
		if (!(key in object)) {
			object[key] = mutation.inner.value;
		}
	} else if (mutation.inner.kind === "prepend") {
		if (!(key in object) || object[key] === undefined) {
			object[key] = [];
		}
		let array = object[key];
		assert_(array instanceof Array);
		object[key] = [...flatten(mutation.inner.values), ...array];
	} else if (mutation.inner.kind === "append") {
		if (!(key in object) || object[key] === undefined) {
			object[key] = [];
		}
		let array = object[key];
		assert_(array instanceof Array);
		object[key] = [...array, ...flatten(mutation.inner.values)];
	} else if (mutation.inner.kind === "prefix") {
		if (!(key in object)) {
			object[key] = await template();
		}
		let value = object[key];
		assert_(
			value === undefined ||
				typeof value === "string" ||
				Artifact.is(value) ||
				value instanceof Template,
		);
		object[key] = await Template.join(
			mutation.inner.separator,
			mutation.inner.template,
			value,
		);
	} else if (mutation.inner.kind === "suffix") {
		if (!(key in object)) {
			object[key] = await template();
		}
		let value = object[key];
		assert_(
			value === undefined ||
				typeof value === "string" ||
				Artifact.is(value) ||
				value instanceof Template,
		);
		object[key] = await Template.join(
			mutation.inner.separator,
			value,
			mutation.inner.template,
		);
	}
};
