import * as tg from "./index.ts";

export function run<
	A extends tg.UnresolvedArgs<Array<tg.Value>>,
	R extends tg.ReturnValue,
>(function_: (...args: A) => R): tg.RunBuilder<[], tg.ResolvedReturnValue<R>>;
export function run<
	A extends tg.UnresolvedArgs<Array<tg.Value>>,
	R extends tg.ReturnValue,
>(
	function_: (...args: A) => R,
	...args: tg.UnresolvedArgs<tg.ResolvedArgs<A>>
): tg.RunBuilder<[], tg.ResolvedReturnValue<R>>;
export function run(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<tg.Template.Arg>
): tg.RunBuilder;
export function run(...args: tg.Args<tg.Process.RunArg>): tg.RunBuilder;
export function run(...args: any): any {
	if (typeof args[0] === "function") {
		return new RunBuilder({
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
		return new RunBuilder(arg);
	} else {
		return new RunBuilder(...args);
	}
}

async function inner(...args: tg.Args<tg.Process.RunArg>): Promise<tg.Value> {
	let cwd =
		tg.Process.current !== undefined
			? (await tg.Process.current.cwd()) !== undefined
				? tg.process.cwd
				: undefined
			: tg.process.cwd;
	let env =
		tg.Process.current !== undefined
			? await tg.Process.current.env()
			: { ...tg.process.env };
	delete env.TANGRAM_OUTPUT;
	delete env.TANGRAM_PROCESS;
	delete env.TANGRAM_URL;
	let arg = await arg_(
		{
			cwd,
			env,
		},
		...args,
	);

	let currentCommand = await tg.Process.current?.command();
	let sourceOptions: tg.Referent.Options = {};
	if ("name" in arg) {
		sourceOptions.name = arg.name;
	}
	if (
		"executable" in arg &&
		typeof arg.executable === "object" &&
		"module" in arg.executable
	) {
		sourceOptions = {
			...arg.executable.module.referent.options,
			...sourceOptions,
		};
		arg.executable.module.referent.options = {};
	}

	let checksum = arg.checksum;
	let processMounts: Array<tg.Process.Mount> = [];
	let commandMounts: Array<tg.Command.Mount> | undefined;
	if ("mounts" in arg && arg.mounts !== undefined) {
		for (let mount of arg.mounts) {
			if (tg.Artifact.is(mount.source)) {
				if (commandMounts === undefined) {
					commandMounts = [];
				}
				commandMounts.push(mount as tg.Command.Mount);
			} else {
				processMounts.push(mount as tg.Process.Mount);
			}
		}
	} else {
		commandMounts = await currentCommand?.mounts();
		processMounts = tg.Process.current?.state?.mounts ?? [];
	}
	let processStdin = tg.Process.current?.state?.stdin;
	let commandStdin: tg.Blob.Arg | undefined;
	if ("stdin" in arg) {
		processStdin = undefined;
		if (arg.stdin !== undefined) {
			commandStdin = arg.stdin;
		}
	} else {
		commandStdin = await currentCommand?.stdin();
	}
	let stdout = tg.Process.current?.state?.stdout;
	if ("stdout" in arg) {
		stdout = arg.stdout;
	}
	let stderr = tg.Process.current?.state?.stderr;
	if ("stderr" in arg) {
		stderr = arg.stderr;
	}
	let command = await tg.command(
		"args" in arg ? { args: arg.args } : undefined,
		"cwd" in arg ? { cwd: arg.cwd } : undefined,
		"env" in arg ? { env: arg.env } : undefined,
		"executable" in arg ? { executable: arg.executable } : undefined,
		"host" in arg ? { host: arg.host } : undefined,
		"user" in arg ? { user: arg.user } : undefined,
		commandMounts !== undefined ? { mounts: commandMounts } : undefined,
		commandStdin !== undefined ? { stdin: commandStdin } : undefined,
	);

	let network =
		"network" in arg
			? (arg.network ?? false)
			: (tg.Process.current?.state?.network ?? false);
	let commandId = await command.store();
	let commandReferent = {
		item: commandId,
		options: sourceOptions,
	};
	let spawnOutput = await tg.handle.spawnProcess({
		checksum,
		command: commandReferent,
		create: false,
		mounts: processMounts,
		network,
		parent: undefined,
		remote: undefined,
		retry: false,
		stderr,
		stdin: processStdin,
		stdout,
	});
	let process = new tg.Process({
		id: spawnOutput.process,
		remote: spawnOutput.remote,
		state: undefined,
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
	...args: tg.Args<tg.Process.RunArg>
): Promise<tg.Process.RunArgObject> {
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
				let executable = tg.process.env.SHELL ?? "sh";
				return {
					args: ["-c", arg],
					executable,
					host,
				};
			} else if (arg instanceof tg.Command) {
				let object = await arg.object();
				let output: tg.Process.RunArgObject = {
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

export interface RunBuilder<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
> {
	// biome-ignore lint/style/useShorthandFunctionType: This is necessary to make this callable.
	(...args: tg.UnresolvedArgs<A>): RunBuilder<[], R>;
}

// biome-ignore lint/suspicious/noUnsafeDeclarationMerging: This is necessary to make this callable.
export class RunBuilder<
	// biome-ignore lint/correctness/noUnusedVariables: <reason>
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
> extends Function {
	#args: tg.Args<tg.Process.RunArg>;

	constructor(...args: tg.Args<tg.Process.RunArg>) {
		super();
		this.#args = args;
		// biome-ignore lint/correctness/noConstructorReturn: This is necessary to make this callable.
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

	mount(
		...mounts: Array<tg.Unresolved<tg.Command.Mount | tg.Process.Mount>>
	): this {
		this.#args.push({ mounts });
		return this;
	}

	mounts(
		...mounts: Array<
			tg.Unresolved<
				tg.MaybeMutation<Array<tg.Command.Mount | tg.Process.Mount>>
			>
		>
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

	// biome-ignore lint/suspicious/noThenProperty: <reason>
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
