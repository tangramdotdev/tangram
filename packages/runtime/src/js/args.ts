import { Artifact } from "./artifact.ts";
import { assert } from "./assert.ts";
import { Mutation } from "./mutation.ts";
import { Unresolved, resolve } from "./resolve.ts";
import { Template, template } from "./template.ts";
import {
	MaybeMutation,
	MaybeMutationMap,
	MaybeNestedArray,
	MutationMap,
	flatten,
} from "./util.ts";
import { Value } from "./value.ts";

export type Args<T extends Value = Value> = Array<
	Unresolved<MaybeNestedArray<MaybeMutationMap<T>>>
>;

export namespace Args {
	export let apply = async <
		A extends Value = Value,
		R extends { [key: string]: Value } = { [key: string]: Value },
	>(
		args: Args<A>,
		map: (
			arg: MaybeMutationMap<Exclude<A, Array<Value>>>,
		) => Promise<MaybeNestedArray<MutationMap<R>>>,
	): Promise<Partial<R>> => {
		return flatten(
			await Promise.all(
				flatten(await Promise.all(args.map(resolve))).map((arg) =>
					map(arg as unknown as MaybeMutationMap<Exclude<A, Array<Value>>>),
				),
			),
		).reduce(async (object, mutations) => {
			for (let [key, mutation] of Object.entries(mutations)) {
				await mutate(await object, key, mutation);
			}
			return object;
		}, Promise.resolve({}));
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
	} else if (mutation.inner.kind === "array_prepend") {
		if (!(key in object) || object[key] === undefined) {
			object[key] = [];
		}
		let array = object[key];
		assert(array instanceof Array);
		object[key] = [...flatten(mutation.inner.values), ...array];
	} else if (mutation.inner.kind === "array_append") {
		if (!(key in object) || object[key] === undefined) {
			object[key] = [];
		}
		let array = object[key];
		assert(array instanceof Array);
		object[key] = [...array, ...flatten(mutation.inner.values)];
	} else if (mutation.inner.kind === "template_prepend") {
		if (!(key in object)) {
			object[key] = await template();
		}
		let value = object[key];
		assert(
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
	} else if (mutation.inner.kind === "template_append") {
		if (!(key in object)) {
			object[key] = await template();
		}
		let value = object[key];
		assert(
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
