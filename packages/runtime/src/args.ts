import * as tg from "./index.ts";
import { mergeMaybeMutationMaps } from "./mutation.ts";

export type Args<T extends tg.Value = tg.Value> = Array<
	tg.Unresolved<tg.ValueOrMaybeMutationMap<T>>
>;

export namespace Args {
	export type Rules<
		T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
	> = {
		[K in keyof T]:
			| tg.Mutation.Kind
			| ((arg: T[K]) => tg.MaybePromise<tg.Mutation<T[K]>>);
	};

	export let apply = async <
		T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
	>(
		args: Array<tg.MaybeMutationMap<T>>,
		rules?: Rules<T>,
	): Promise<T> => {
		let mutations = await createMutations(args, rules);
		let arg = await mergeMaybeMutationMaps(mutations);
		return arg;
	};

	let createMutations = async <
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
									value instanceof tg.Template ||
										tg.Artifact.is(value) ||
										typeof value === "string",
								);
								object[key] = await tg.Mutation.prefix(value);
								break;
							case "suffix":
								tg.assert(
									value instanceof tg.Template ||
										tg.Artifact.is(value) ||
										typeof value === "string",
								);
								object[key] = await tg.Mutation.suffix(value);
								break;
							case "merge":
								object[key] = await tg.Mutation.merge(value);
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
}
