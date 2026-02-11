import * as tg from "./index.ts";

export function build<
	A extends tg.UnresolvedArgs<Array<tg.Value>>,
	R extends tg.ReturnValue,
>(function_: (...args: A) => R): tg.BuildBuilder<[], tg.ResolvedReturnValue<R>>;
export function build<
	A extends tg.UnresolvedArgs<Array<tg.Value>>,
	R extends tg.ReturnValue,
>(
	function_: (...args: A) => R,
	...args: tg.UnresolvedArgs<tg.ResolvedArgs<A>>
): tg.BuildBuilder<[], tg.ResolvedReturnValue<R>>;
export function build(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<tg.Template.Arg>
): tg.BuildBuilder;
export function build(...args: tg.Args<tg.Process.BuildArg>): tg.BuildBuilder;
export function build(...args: any): any {
	if (typeof args[0] === "function") {
		return new tg.BuildBuilder({
			host: "js",
			executable: tg.Command.Executable.fromData(tg.handle.magic(args[0])),
			args: args.slice(1),
		});
	} else if (Array.isArray(args[0]) && "raw" in args[0]) {
		let strings = args[0] as TemplateStringsArray;
		let placeholders = args.slice(1);
		let template = tg.template(strings, ...placeholders);
		let executable = tg.process.env.SHELL ?? "sh";
		tg.assert(tg.Command.Arg.Executable.is(executable));
		let arg = {
			executable,
			args: ["-c", template],
		};
		return new tg.BuildBuilder(arg);
	} else {
		return new tg.BuildBuilder(...args);
	}
}

async function inner(...args: tg.Args<tg.Process.BuildArg>): Promise<tg.Value> {
	let arg = await arg_(
		{
			env: {
				TANGRAM_HOST: tg.process.env.TANGRAM_HOST,
			},
		},
		...args,
	);

	let sourceOptions: tg.Referent.Options = {};
	if ("name" in arg) {
		sourceOptions.name = arg.name;
	}
	let executable: tg.Command.Arg.Executable | undefined;
	if ("executable" in arg) {
		if (
			typeof arg.executable === "object" &&
			!tg.Artifact.is(arg.executable) &&
			"module" in arg.executable
		) {
			sourceOptions = {
				...arg.executable.module.referent.options,
				...sourceOptions,
			};
			executable = {
				...arg.executable,
				module: {
					...arg.executable.module,
					referent: {
						...arg.executable.module.referent,
						options: {},
					},
				},
			};
		} else {
			executable = arg.executable;
		}
	}

	let commandMounts: Array<tg.Command.Mount> | undefined;
	if ("mounts" in arg && arg.mounts !== undefined) {
		commandMounts = arg.mounts;
	}
	let commandStdin: tg.Blob.Arg | undefined;
	if ("stdin" in arg && arg.stdin !== undefined) {
		commandStdin = arg.stdin;
	}
	let command = await tg.command(
		"args" in arg ? { args: arg.args } : undefined,
		"cwd" in arg ? { cwd: arg.cwd } : undefined,
		"env" in arg ? { env: arg.env } : undefined,
		executable !== undefined ? { executable: executable } : undefined,
		"host" in arg ? { host: arg.host } : undefined,
		"user" in arg ? { user: arg.user } : undefined,
		commandMounts !== undefined ? { mounts: commandMounts } : undefined,
		commandStdin !== undefined ? { stdin: commandStdin } : undefined,
	);

	let checksum = arg.checksum;
	let network = "network" in arg ? (arg.network ?? false) : false;
	if (network === true && checksum === undefined) {
		throw new Error("a checksum is required to build with network enabled");
	}
	let commandId = await command.store();
	let commandReferent = {
		item: commandId,
		options: sourceOptions,
	};
	let spawnOutput = await tg.handle.spawnProcess({
		checksum,
		command: commandReferent,
		create: false,
		mounts: [],
		network,
		parent: undefined,
		remote: undefined,
		retry: false,
		stderr: undefined,
		stdin: undefined,
		stdout: undefined,
	});
	let process = new tg.Process({
		id: spawnOutput.process,
		remote: spawnOutput.remote,
		state: undefined,
		token: spawnOutput.token,
	});

	let wait = await process.wait();

	if (wait.error !== undefined) {
		let error = wait.error;
		const source = {
			item: error,
			options: sourceOptions,
		};
		const values: { [key: string]: string } = {
			id: process.id,
		};
		if (sourceOptions.name !== undefined) {
			values.name = sourceOptions.name;
		}
		throw tg.error("the child process failed", {
			source,
			values,
		});
	}
	if (wait.exit >= 1 && wait.exit < 128) {
		const error = tg.error(`the process exited with code ${wait.exit}`);
		const source = {
			item: error,
			options: sourceOptions,
		};
		const values: { [key: string]: string } = {
			id: process.id,
		};
		if (sourceOptions.name !== undefined) {
			values.name = sourceOptions.name;
		}
		throw tg.error("the child process failed", {
			source,
			values,
		});
	}
	if (wait.exit >= 128) {
		const error = tg.error(`the process exited with code ${wait.exit}`);
		const source = {
			item: error,
			options: sourceOptions,
		};
		const values: { [key: string]: string } = {
			id: process.id,
		};
		if (sourceOptions.name !== undefined) {
			values.name = sourceOptions.name;
		}
		throw tg.error(`the child process exited with signal ${wait.exit - 128}`, {
			source,
			values,
		});
	}

	return wait.output;
}

async function arg_(
	...args: tg.Args<tg.Process.BuildArg>
): Promise<tg.Process.BuildArgObject> {
	return await tg.Args.apply({
		args,
		map: async (arg) => {
			if (arg === undefined) {
				return {};
			} else if (
				typeof arg === "string" ||
				tg.Artifact.is(arg) ||
				arg instanceof tg.Template
			) {
				let host = tg.process.env.TANGRAM_HOST;
				tg.assert(host !== undefined, "TANGRAM_HOST must be set");
				let executable = tg.process.env.SHELL ?? "sh";
				return {
					args: ["-c", arg],
					executable,
					host,
				};
			} else if (arg instanceof tg.Command) {
				let object = await arg.object();
				let output: tg.Process.BuildArgObject = {
					args: object.args,
					env: object.env,
					executable: object.executable,
					host: object.host,
					mounts: object.mounts,
				};
				if (object.cwd !== undefined) {
					output.cwd = object.cwd;
				}
				if (object.stdin !== undefined) {
					output.stdin = object.stdin;
				}
				if (object.user !== undefined) {
					output.user = object.user;
				}
				return output;
			} else {
				return arg;
			}
		},
		reduce: {
			args: "append",
			env: "merge",
		},
	});
}

export interface BuildBuilder<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
> {
	(...args: tg.UnresolvedArgs<A>): tg.BuildBuilder<[], R>;
}

export class BuildBuilder<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
> extends Function {
	#args: Array<tg.Unresolved<tg.MaybeMutationMap<tg.Process.BuildArgObject>>>;

	constructor(...args: tg.Args<tg.Process.BuildArgObject>) {
		super();
		this.#args = args;
		return new Proxy(this, {
			get(this_: any, prop, _receiver) {
				if (typeof this_[prop] === "function") {
					return this_[prop].bind(this_);
				}
				return this_[prop];
			},
			apply: (this_, _, args) => {
				return this_.args(args);
			},
			getPrototypeOf: (this_) => {
				return Object.getPrototypeOf(this_);
			},
		});
	}

	arg(...args: Array<tg.Unresolved<tg.Value>>): this {
		this.#args.push({ args });
		return this;
	}

	args(...args: Array<tg.Unresolved<tg.MaybeMutation<Array<tg.Value>>>>): this {
		this.#args.push(...args.map((args) => ({ args })));
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

	env(
		...envs: Array<tg.Unresolved<tg.MaybeMutation<tg.MaybeMutationMap>>>
	): this {
		this.#args.push(...envs.map((env) => ({ env })));
		return this;
	}

	executable(
		executable: tg.Unresolved<tg.MaybeMutation<tg.Command.Arg.Executable>>,
	): this {
		this.#args.push({ executable });
		return this;
	}

	host(host: tg.Unresolved<tg.MaybeMutation<string>>): this {
		this.#args.push({ host });
		return this;
	}

	mount(...mounts: Array<tg.Unresolved<tg.Command.Mount>>): this {
		this.#args.push({ mounts });
		return this;
	}

	mounts(
		...mounts: Array<tg.Unresolved<tg.MaybeMutation<Array<tg.Command.Mount>>>>
	): this {
		this.#args.push(...mounts.map((mounts) => ({ mounts })));
		return this;
	}

	named(name: tg.Unresolved<tg.MaybeMutation<string | undefined>>): this {
		this.#args.push({ name });
		return this;
	}

	network(network: tg.Unresolved<tg.MaybeMutation<boolean>>): this {
		this.#args.push({ network });
		return this;
	}

	then<TResult1 = R, TResult2 = never>(
		onfulfilled?:
			| ((value: R) => TResult1 | PromiseLike<TResult1>)
			| undefined
			| null,
		onrejected?:
			| ((reason: any) => TResult2 | PromiseLike<TResult2>)
			| undefined
			| null,
	): PromiseLike<TResult1 | TResult2> {
		return inner(...this.#args)
			.then((output) => output as R)
			.then(onfulfilled, onrejected);
	}
}
