import * as tg from "./index.ts";

export class BuildBuilder {
	#args: Array<tg.Unresolved<tg.MaybeMutationMap<tg.Process.BuildArgObject>>>;

	constructor(...args: tg.Args<tg.Process.BuildArgObject>) {
		this.#args = args;
	}

	args(args: tg.Unresolved<tg.MaybeMutation<Array<tg.Value>>>): this {
		this.#args.push({ args });
		return this;
	}

	checksum(
		checksum: tg.Unresolved<tg.MaybeMutation<tg.Checksum | undefined>>,
	): this {
		this.#args.push({ checksum });
		return this;
	}

	cwd(cwd: tg.Unresolved<tg.MaybeMutation<string | undefined>>): this {
		this.#args.push({ cwd });
		return this;
	}

	env(env: tg.Unresolved<tg.MaybeMutation<tg.MaybeMutationMap>>): this {
		this.#args.push({ env });
		return this;
	}

	executable(
		executable: tg.Unresolved<tg.MaybeMutation<tg.Command.ExecutableArg>>,
	): this {
		this.#args.push({ executable });
		return this;
	}

	host(host: tg.Unresolved<tg.MaybeMutation<string>>): this {
		this.#args.push({ host });
		return this;
	}

	mount(
		mounts: tg.Unresolved<
			tg.MaybeMutation<Array<string | tg.Template | tg.Command.Mount>>
		>,
	): this {
		this.#args.push({ mounts });
		return this;
	}

	network(network: tg.Unresolved<tg.MaybeMutation<boolean>>): this {
		this.#args.push({ network });
		return this;
	}

	// @ts-ignore
	// biome-ignore lint/suspicious/noThenProperty: promiseLike class
	then<TResult1 = tg.Value, TResult2 = never>(
		onfulfilled?:
			| ((value: tg.Value) => TResult1 | PromiseLike<TResult1>)
			| undefined
			| null,
		onrejected?:
			| ((reason: any) => TResult2 | PromiseLike<TResult2>)
			| undefined
			| null,
	): PromiseLike<TResult1 | TResult2> {
		return tg
			.build(...(this.#args as tg.Args<tg.Process.BuildArgObject>))
			.then(onfulfilled, onrejected);
	}
}
