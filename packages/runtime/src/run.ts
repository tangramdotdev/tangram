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
		return tg.command(...args).then((command) => command.run());
	} else if (Array.isArray(args[0]) && "raw" in args[0]) {
		let strings = args[0] as TemplateStringsArray;
		let placeholders = args.slice(1);
		let template = tg.template(strings, ...placeholders);
		let arg = {
			executable: "/bin/sh",
			args: ["-c", template],
		};
		return new RunBuilder(arg);
	} else {
		return inner(...args);
	}
}

async function inner(...args: tg.Args<tg.Process.RunArg>): Promise<tg.Value> {
	let state = tg.Process.current.state!;
	let currentCommand = await tg.Process.current.command();
	let arg = await arg_(
		{
			cwd: currentCommand.cwd(),
			env: currentCommand.env(),
		},
		...args,
	);
	let checksum = arg.checksum;
	let processMounts: Array<tg.Process.Mount> = [];
	let commandMounts: Array<tg.Command.Mount> | undefined;
	if ("mounts" in arg && arg.mounts !== undefined) {
		for (let mount of arg.mounts) {
			if (typeof mount === "string" || mount instanceof tg.Template) {
				try {
					let commandMount = await tg.Command.Mount.parse(mount);
					if (commandMounts === undefined) {
						commandMounts = [];
					}
					commandMounts.push(commandMount);
				} catch {}
				try {
					let processMount = await tg.Process.Mount.parse(mount);
					processMounts.push(processMount);
				} catch {}
			} else {
				if (tg.Artifact.is(mount.source)) {
					if (commandMounts === undefined) {
						commandMounts = [];
					}
					commandMounts.push(mount as tg.Command.Mount);
				} else {
					processMounts.push(mount as tg.Process.Mount);
				}
			}
		}
	} else {
		commandMounts = await currentCommand.mounts();
		processMounts = state.mounts;
	}
	let processStdin = state.stdin;
	let commandStdin: tg.Blob.Arg | undefined = undefined;
	if ("stdin" in arg) {
		processStdin = undefined;
		if (arg.stdin !== undefined) {
			commandStdin = arg.stdin;
		}
	} else {
		commandStdin = await currentCommand.stdin();
	}
	let stderr = state.stdout;
	if ("stderr" in arg) {
		stderr = arg.stderr;
	}
	let stdout = state.stdout;
	if ("stdout" in arg) {
		stdout = arg.stdout;
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
	let network = "network" in arg ? arg.network : state.network;
	let commandId = await command.id();
	let output = await syscall("process_spawn", {
		checksum,
		command: commandId,
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
		id: output.process,
		remote: output.remote,
		state: undefined,
	});
	let wait = await process.wait();
	if (wait.error) {
		throw wait.error;
	}
	if (wait.exit >= 1 && wait.exit < 128) {
		throw new Error(`the process exited with code ${wait.exit}`);
	}
	if (wait.exit >= 128) {
		throw new Error(`the process exited with signal ${wait.exit - 128}`);
	}
	return wait.output;
}

async function arg_(
	...args: tg.Args<tg.Process.RunArg>
): Promise<tg.Process.RunArgObject> {
	let resolved = await Promise.all(args.map(tg.resolve));
	let objects = await Promise.all(
		resolved.map(async (arg) => {
			if (arg === undefined) {
				return {};
			} else if (
				typeof arg === "string" ||
				tg.Artifact.is(arg) ||
				arg instanceof tg.Template
			) {
				return {
					args: ["-c", arg],
					executable: "/bin/sh",
					host: (await (await tg.Process.current.command()).env())!
						.TANGRAM_HOST,
				};
			} else if (arg instanceof tg.Command) {
				let object = await arg.object();
				let output: tg.Process.RunArgObject = {
					args: object.args,
					env: object.env,
					executable: object.executable,
					host: object.host,
				};
				if (object.cwd !== undefined) {
					output.cwd = object.cwd;
				}
				if (object.mounts !== undefined) {
					output.mounts = object.mounts;
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
		}),
	);
	let arg = await tg.Args.apply(objects, {
		args: "append",
		env: "merge",
	});
	return arg;
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
			tg.MaybeMutation<
				Array<string | tg.Template | tg.Command.Mount | tg.Process.Mount>
			>
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
		return tg
			.run(...this.#args)
			.then((output) => output as R)
			.then(onfulfilled, onrejected);
	}
}
