import * as tg from "./index.ts";

export type Args<T = tg.Value> = Array<
	tg.Unresolved<tg.ValueOrMaybeMutationMap<T>>
>;

export namespace Args {
	type Input<T, Mapped extends object, Output extends object> = {
		args: tg.Args<T>;
		map: (arg: tg.ValueOrMaybeMutationMap<T>) => tg.MaybePromise<Mapped>;
		reduce: {
			[K in keyof Output]:
				| (Exclude<Output[K], undefined> extends tg.Value
						? tg.Mutation.Kind
						: never)
				| ((
						a: Output[K] | undefined,
						b: Exclude<K extends keyof Mapped ? Mapped[K] : never, tg.Mutation>,
				  ) => tg.MaybePromise<Output[K]>);
		};
	};

	export let apply = async <T, Mapped extends object, Output extends object>(
		input: Input<T, Mapped, Output>,
	): Promise<Output> => {
		let { args, map, reduce } = input;
		let resolved = (await Promise.all(args.map(tg.resolve))) as Array<
			tg.ValueOrMaybeMutationMap<T>
		>;
		return await applyResolved({ args: resolved, map, reduce });
	};

	export let applyResolved = async <
		T,
		Mapped extends object,
		Output extends object,
	>(
		input: Omit<Input<T, Mapped, Output>, "args"> & {
			args: Array<tg.ValueOrMaybeMutationMap<T>>;
		},
	): Promise<Output> => {
		let { args, map, reduce } = input;
		let output: { [key: string]: unknown } = {};
		for (let arg of args) {
			let object = await map(arg);
			for (let [key, value] of Object.entries(object)) {
				if (value === undefined) {
					continue;
				} else if (value === null) {
					// An explicit null overrides any accumulated value, clearing it.
					output[key] = null;
				} else if (value instanceof tg.Mutation) {
					let current = output[key];
					tg.assert(current === undefined || tg.Value.is(current));
					let next = await value.apply(current);
					if (next === undefined) {
						delete output[key];
					} else {
						output[key] = next;
					}
				} else {
					let reducer = reduce[key as keyof Output];
					if (typeof reducer === "string") {
						let mutation: tg.Mutation;
						switch (reducer) {
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
								mutation = await tg.Mutation.prefix(value);
								break;
							case "suffix":
								tg.assert(
									value instanceof tg.Template ||
										tg.Artifact.is(value) ||
										typeof value === "string",
								);
								mutation = await tg.Mutation.suffix(value);
								break;
							case "merge":
								mutation = await tg.Mutation.merge(value);
								break;
							default:
								return tg.unreachable(`unknown mutation kind "${reducer}"`);
						}
						let current = output[key];
						tg.assert(current === undefined || tg.Value.is(current));
						let next = await mutation.apply(current);
						if (next === undefined) {
							delete output[key];
						} else {
							output[key] = next;
						}
					} else if (reducer !== undefined) {
						let reduce_ = reducer as (
							a: unknown,
							b: unknown,
						) => tg.MaybePromise<unknown>;
						output[key] = await reduce_(output[key], value);
					} else {
						output[key] = value;
					}
				}
			}
		}
		return output as Output;
	};
}
