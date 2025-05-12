import * as tg from "./index.ts";

export type Args<T extends tg.Value = tg.Value> = Array<
	tg.Unresolved<tg.ValueOrMaybeMutationMap<T>>
>;

export namespace Args {
	type Input<T extends tg.Value, O extends { [key: string]: tg.Value }> = {
		args: tg.Args<T>;
		map: (
			arg: tg.ValueOrMaybeMutationMap<T>,
		) => tg.MaybePromise<tg.MaybeMutationMap<O>>;
		reduce: {
			[K in keyof O]:
				| tg.Mutation.Kind
				| ((a: O[K] | undefined, b: O[K]) => tg.MaybePromise<O[K]>);
		};
	};

	export let apply = async <
		T extends tg.Value,
		O extends { [key: string]: tg.Value },
	>(
		input: Input<T, O>,
	): Promise<O> => {
		let { args, map, reduce } = input;
		let resolved = (await Promise.all(args.map(tg.resolve))) as Array<
			tg.ValueOrMaybeMutationMap<T>
		>;
		let output: { [key: string]: tg.Value } = {};
		for (let arg of resolved) {
			let object = await map(arg);
			for (let [key, value] of Object.entries(object)) {
				if (value instanceof tg.Mutation) {
					await value.apply(output, key);
				} else if (reduce[key] !== undefined) {
					if (typeof reduce[key] === "string") {
						let mutation: tg.Mutation;
						switch (reduce[key]) {
							case "set":
								mutation = await tg.Mutation.set(value);
								break;
							case "unset":
								mutation = tg.Mutation.unset();
								break;
							case "set_if_unset":
								mutation = await tg.Mutation.setIfUnset(value);
								break;
							case "prepend":
								mutation = await tg.Mutation.prepend(value);
								break;
							case "append":
								mutation = await tg.Mutation.append(value);
								break;
							case "prefix":
								tg.assert(
									value instanceof tg.Template ||
										tg.Artifact.is(value) ||
										typeof value === "string",
								);
								mutation = await tg.Mutation.prefix<tg.Template.Arg>(value);
								break;
							case "suffix":
								tg.assert(
									value instanceof tg.Template ||
										tg.Artifact.is(value) ||
										typeof value === "string",
								);
								mutation = await tg.Mutation.suffix<tg.Template.Arg>(value);
								break;
							case "merge":
								mutation = await tg.Mutation.merge(value);
								break;
							default:
								return tg.unreachable(`unknown mutation kind "${reduce[key]}"`);
						}
						await mutation.apply(output, key);
					} else {
						output[key] = await reduce[key](
							output[key] as O[typeof key] | undefined,
							value,
						);
					}
				} else {
					output[key] = value;
				}
			}
		}
		return output as O;
	};
}
