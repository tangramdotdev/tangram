import * as tg from "./index.ts";
import { Template, template } from "./template.ts";
import { flatten } from "./util.ts";

export type Args<T extends tg.Value = tg.Value> = Array<
	tg.Unresolved<tg.MaybeNestedArray<tg.ValueOrMaybeMutationMap<T>>>
>;

export namespace Args {
	export type Rules<
		T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
	> = {
		[K in keyof T]:
			| tg.Mutation.Kind
			| ((arg: T[K]) => tg.MaybePromise<tg.Mutation<T[K]>>);
	};

	export let createMutations = async <
		T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
		R extends { [key: string]: tg.Value } = T,
	>(
		args: Array<tg.MaybeMutationMap<T>>,
		rules?: Rules<T>,
	): Promise<Array<tg.MutationMap<R>>> => {
		return await Promise.all(
			args.map(async (arg) => {
				let object: { [key: string]: tg.Mutation<T> } = {};
				for (let [key, value] of Object.entries(arg)) {
					if (value instanceof tg.Mutation) {
						object[key] = value;
						continue;
					}
					let mutation = rules !== undefined ? rules[key] : undefined;
					if (mutation === undefined) {
						object[key] = await tg.Mutation.set<typeof value>(value);
					} else if (typeof mutation === "string") {
						switch (mutation) {
							case "set":
								object[key] = await tg.Mutation.set(value);
								break;
							case "unset":
								object[key] = tg.Mutation.unset();
								break;
							case "set_if_unset":
								object[key] = await tg.Mutation.setIfUnset(value);
								break;
							case "prepend":
								object[key] = await tg.Mutation.prepend(value);
								break;
							case "append":
								object[key] = await tg.Mutation.append(value);
								break;
							case "prefix":
								tg.assert(
									value instanceof Template ||
										tg.Artifact.is(value) ||
										typeof value === "string",
								);
								object[key] = await tg.Mutation.prefix(value);
								break;
							case "suffix":
								tg.assert(
									value instanceof Template ||
										tg.Artifact.is(value) ||
										typeof value === "string",
								);
								object[key] = await tg.Mutation.suffix(value);
								break;
							default:
								return tg.unreachable(`unknown mutation kind "${mutation}"`);
						}
					} else {
						object[key] = await mutation(value);
					}
				}
				return object as tg.MutationMap<T>;
			}),
		);
	};

	export let applyMutations = async <
		T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
	>(
		mutations: Array<tg.MaybeMutationMap<T>>,
	): Promise<T> => {
		return await mutations.reduce(
			async (object, mutations) => {
				if (mutations === undefined) {
					return Promise.resolve({}) as Promise<T>;
				}
				for (let [key, mutation] of Object.entries(mutations)) {
					await mutate(await object, key, mutation);
				}
				return object;
			},
			Promise.resolve({}) as Promise<T>,
		);
	};
}

export let mutate = async (
	object: { [key: string]: tg.Value },
	key: string,
	mutation: tg.MaybeMutation,
) => {
	if (!(mutation instanceof tg.Mutation)) {
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
		tg.assert(array instanceof Array);
		object[key] = [...flatten(mutation.inner.values), ...array];
	} else if (mutation.inner.kind === "append") {
		if (!(key in object) || object[key] === undefined) {
			object[key] = [];
		}
		let array = object[key];
		tg.assert(array instanceof Array);
		object[key] = [...array, ...flatten(mutation.inner.values)];
	} else if (mutation.inner.kind === "prefix") {
		if (!(key in object)) {
			object[key] = await template();
		}
		let value = object[key];
		tg.assert(
			value === undefined ||
				typeof value === "string" ||
				tg.Artifact.is(value) ||
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
		tg.assert(
			value === undefined ||
				typeof value === "string" ||
				tg.Artifact.is(value) ||
				value instanceof Template,
		);
		object[key] = await Template.join(
			mutation.inner.separator,
			value,
			mutation.inner.template,
		);
	}
};
