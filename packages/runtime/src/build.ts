import * as tg from "./index.ts";

export function build<A extends Array<tg.Value>, R extends tg.Value>(
	function_: (...args: tg.UnresolvedArray<A>) => tg.Unresolved<R>,
): tg.BuildBuilder<A, R>;
export function build<A extends Array<tg.Value>, R extends tg.Value>(
	function_: (...args: tg.UnresolvedArray<A>) => tg.Unresolved<R>,
	...args: tg.UnresolvedArray<A>
): tg.BuildBuilder<[], R>;
export function build(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<tg.Template.Arg>
): tg.BuildBuilder;
export function build(...args: tg.Args<tg.Process.BuildArg>): tg.BuildBuilder;
export function build(...args: any): any {
	if (typeof args[0] === "function") {
		return tg.command(...args).then((command) => command.build());
	} else if (Array.isArray(args[0]) && "raw" in args[0]) {
		let strings = args[0] as TemplateStringsArray;
		let placeholders = args.slice(1);
		let template = tg.template(strings, ...placeholders);
		let arg = {
			executable: "/bin/sh",
			args: ["-c", template],
		};
		return new BuildBuilder(arg);
	} else {
		return inner(...args);
	}
}

async function inner(...args: tg.Args<tg.Process.BuildArg>): Promise<tg.Value> {
	let arg = await arg_(
		{
			env: {
				TANGRAM_HOST: (await (await tg.Process.current.command()).env())!
					.TANGRAM_HOST,
			},
		},
		...args,
	);
	let commandMounts: Array<tg.Command.Mount> | undefined;
	if ("mounts" in arg && arg.mounts !== undefined) {
		commandMounts = await Promise.all(
			arg.mounts.map(async (mount) => {
				if (typeof mount === "string" || mount instanceof tg.Template) {
					return await tg.Command.Mount.parse(mount);
				} else {
					return mount;
				}
			}),
		);
	}
	let commandStdin: tg.Blob.Arg | undefined = undefined;
	if ("stdin" in arg && arg.stdin !== undefined) {
		commandStdin = arg.stdin;
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
	let checksum = arg.checksum;
	let network = "network" in arg ? arg.network : false;
	if (network === true && checksum === undefined) {
		throw new Error("a checksum is required to build with network enabled");
	}
	let commandId = await command.id();
	let spawnOutput = await syscall("process_spawn", {
		checksum,
		command: commandId,
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
	...args: tg.Args<tg.Process.BuildArg>
): Promise<tg.Process.BuildArgObject> {
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
				let output: tg.Process.BuildArgObject = {
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

export interface BuildBuilder<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
> {
	// biome-ignore lint/style/useShorthandFunctionType: This is necessary to make this callable.
	(...args: tg.UnresolvedArray<A>): tg.BuildBuilder<[], R>;
}

// biome-ignore lint/suspicious/noUnsafeDeclarationMerging: This is necessary to make this callable.
export class BuildBuilder<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
> extends Function {
	#args: Array<tg.Unresolved<tg.MaybeMutationMap<tg.Process.BuildArgObject>>>;

	constructor(...args: tg.Args<tg.Process.BuildArgObject>) {
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
			tg.MaybeMutation<Array<tg.Template | tg.Command.Mount>>
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
