import * as tg from "./index.ts";

export function command<A extends Array<tg.Value>, R extends tg.Value>(
	function_: (...args: tg.UnresolvedArray<A>) => tg.Unresolved<R>,
): tg.CommandBuilder<A, R>;
export function command<A extends Array<tg.Value>, R extends tg.Value>(
	function_: (...args: tg.UnresolvedArray<A>) => tg.Unresolved<R>,
	...args: tg.UnresolvedArray<A>
): tg.CommandBuilder<[], R>;
export function command(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<tg.Template.Arg>
): tg.CommandBuilder;
export function command(...args: tg.Args<tg.Command.Arg>): tg.CommandBuilder;
export function command(...args: any): any {
	if (typeof args[0] === "function") {
		return new CommandBuilder({
			host: "js",
			executable: syscall("magic", args[0]),
			args: args.slice(1),
		});
	} else if (Array.isArray(args[0]) && "raw" in args[0]) {
		let strings = args[0] as TemplateStringsArray;
		let placeholders = args.slice(1);
		let template = tg.template(strings, ...placeholders);
		let arg = {
			executable: "/bin/sh",
			args: ["-c", template],
		};
		return new CommandBuilder(arg);
	} else {
		return new CommandBuilder(...args);
	}
}

export class Command<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
> {
	#state: Command.State;

	constructor(state: Command.State) {
		this.#state = state;
	}

	get state(): Command.State {
		return this.#state;
	}

	static withId(id: Command.Id): Command {
		return new Command({ id });
	}

	static async new<
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	>(...args: tg.Args<Command.Arg>): Promise<Command<A, R>> {
		let arg = await Command.arg(...args);
		let args_ = arg.args ?? [];
		let cwd = arg.cwd;
		let env = arg.env ?? {};
		let executable: tg.Command.Executable | undefined;
		if (tg.Artifact.is(arg.executable)) {
			executable = { artifact: arg.executable, subpath: undefined };
		} else if (typeof arg.executable === "string") {
			executable = { path: arg.executable };
		} else if (arg.executable !== undefined && "artifact" in arg.executable) {
			executable = {
				artifact: arg.executable.artifact,
				subpath: arg.executable.subpath,
			};
		} else if (arg.executable !== undefined && "module" in arg.executable) {
			executable = {
				module: arg.executable.module,
				target: arg.executable.target,
			};
		} else if (arg.executable !== undefined && "path" in arg.executable) {
			executable = {
				path: arg.executable.path,
			};
		}
		let host =
			arg.host ?? ((await tg.Process.current.env("TANGRAM_HOST")) as string);
		let mounts: Array<tg.Command.Mount> | undefined = undefined;
		if (arg.mounts && arg.mounts.length > 0) {
			mounts = await Promise.all(
				arg.mounts.map(async (mount) => {
					if (typeof mount === "string" || mount instanceof tg.Template) {
						return await tg.Command.Mount.parse(mount);
					} else {
						return mount;
					}
				}),
			);
		}
		if (executable === undefined) {
			throw new Error("cannot create a command without an executable");
		}
		if (host === undefined) {
			throw new Error("cannot create a command without a host");
		}
		let stdin = arg.stdin !== undefined ? await tg.blob(arg.stdin) : undefined;
		let user = arg.user;
		let object = {
			args: args_,
			cwd,
			env,
			executable,
			host,
			mounts,
			stdin,
			user,
		};
		return new Command({ object });
	}

	static async arg(...args: tg.Args<Command.Arg>): Promise<Command.ArgObject> {
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
					const host = await tg.Process.current
						.command()
						.then((command) => command.env())
						.then((env) => env.TANGRAM_HOST);
					return {
						args: ["-c", arg],
						executable: "/bin/sh",
						host,
					};
				} else if (arg instanceof Command) {
					return await arg.object();
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

	static expect(value: unknown): Command {
		tg.assert(value instanceof Command);
		return value;
	}

	static assert(value: unknown): asserts value is Command {
		tg.assert(value instanceof Command);
	}

	async id(): Promise<Command.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<Command.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("object_load", this.#state.id!);
			tg.assert(object.kind === "command");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("object_store", {
				kind: "command",
				value: this.#state.object!,
			});
		}
	}

	async args(): Promise<Array<tg.Value>> {
		return (await this.object()).args;
	}

	async cwd(): Promise<string | undefined> {
		return (await this.object()).cwd;
	}

	async env(): Promise<{ [key: string]: tg.Value }> {
		return (await this.object()).env;
	}

	async executable(): Promise<tg.Command.Executable> {
		return (await this.object()).executable;
	}

	async host(): Promise<string> {
		return (await this.object()).host;
	}

	async stdin(): Promise<tg.Blob | undefined> {
		return (await this.object()).stdin;
	}

	async user(): Promise<string | undefined> {
		return (await this.object()).user;
	}

	async mounts(): Promise<Array<tg.Command.Mount> | undefined> {
		return (await this.object()).mounts;
	}

	build(...args: tg.UnresolvedArray<A>): tg.BuildBuilder<[], R> {
		return tg.build(this, { args });
	}

	run(...args: tg.UnresolvedArray<A>): tg.RunBuilder<[], R> {
		return tg.run(this, { args });
	}
}

export namespace Command {
	export type Arg =
		| undefined
		| string
		| tg.Artifact
		| tg.Template
		| Command
		| ArgObject;

	export type ArgObject = {
		args?: Array<tg.Value> | undefined;
		cwd?: string | undefined;
		env?: tg.MaybeMutationMap | undefined;
		executable?: tg.Command.ExecutableArg | undefined;
		host?: string | undefined;
		mounts?: Array<tg.Template | tg.Command.Mount> | undefined;
		stdin?: tg.Blob.Arg | undefined;
		user?: string | undefined;
	};

	export type Executable =
		| tg.Command.Executable.Artifact
		| tg.Command.Executable.Module
		| tg.Command.Executable.Path;

	export type ExecutableArg =
		| tg.Artifact
		| string
		| tg.Command.Executable.ArtifactArg
		| tg.Command.Executable.ModuleArg
		| tg.Command.Executable.PathArg;

	export namespace Executable {
		export type Artifact = {
			artifact: tg.Artifact;
			subpath: string | undefined;
		};

		export type ArtifactArg = {
			artifact: tg.Artifact;
			subpath?: string | undefined;
		};

		export type Module = {
			module: tg.Module;
			target: string | undefined;
		};

		export type ModuleArg = {
			module: tg.Module;
			target?: string | undefined;
		};

		export type Path = {
			path: string;
		};

		export type PathArg = {
			path: string;
		};
	}

	export type Id = string;

	export type Mount = {
		source: tg.Artifact;
		target: string;
	};

	export namespace Mount {
		export let parse = async (
			t: string | tg.Template,
		): Promise<tg.Command.Mount> => {
			// If the user passed a template, render a string with artifact IDs.
			let s: string | undefined;
			if (typeof t === "string") {
				s = t;
			} else if (t instanceof tg.Template) {
				s = await t.components.reduce(async (acc, component) => {
					if (tg.Artifact.is(component)) {
						return (await acc) + (await component.id());
					} else {
						return (await acc) + component;
					}
				}, Promise.resolve(""));
			} else {
				throw new Error("expected a template or a string");
			}
			tg.assert(s);

			// Handle the readonly/readwrite option if present, rejecting read-write.
			if (s.includes(",")) {
				const [mountPart, option] = s.split(",", 2);
				tg.assert(mountPart);
				tg.assert(option);

				if (option === "ro") {
					s = mountPart;
				} else if (option === "rw") {
					throw new Error("cannot mount artifacts as read/write");
				} else {
					throw new Error(`unknown option: "${option}"`);
				}
			}

			// Split the string into source and target.
			const colonIndex = s.indexOf(":");
			if (colonIndex === -1) {
				throw new Error("expected a target path");
			}

			const sourceId = s.substring(0, colonIndex);
			const source = tg.Artifact.withId(sourceId);
			const target = s.substring(colonIndex + 1);

			// Validate the target is an absolute path.
			if (!target.startsWith("/")) {
				throw new Error(`expected an absolute path: "${target}"`);
			}

			return {
				source,
				target,
			};
		};
	}

	export type Object = {
		args: Array<tg.Value>;
		cwd: string | undefined;
		env: { [key: string]: tg.Value };
		executable: tg.Command.Executable;
		host: string;
		mounts: Array<tg.Command.Mount> | undefined;
		stdin: tg.Blob | undefined;
		user: string | undefined;
	};

	export type State = tg.Object.State<Command.Id, Command.Object>;
}

export interface CommandBuilder<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
> {
	// biome-ignore lint/style/useShorthandFunctionType: This is necessary to make this callable.
	(...args: { [K in keyof A]: tg.Unresolved<A[K]> }): tg.CommandBuilder<[], R>;
}

// biome-ignore lint/suspicious/noUnsafeDeclarationMerging: This is necessary to make this callable.
export class CommandBuilder<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
> extends Function {
	#args: Array<tg.Unresolved<tg.MaybeMutationMap<tg.Command.ArgObject>>>;

	constructor(...args: tg.Args<tg.Command.ArgObject>) {
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

	// @ts-ignore
	// biome-ignore lint/suspicious/noThenProperty: promiseLike class
	then<TResult1 = tg.Command<A, R>, TResult2 = never>(
		onfulfilled?:
			| ((value: tg.Command<A, R>) => TResult1 | PromiseLike<TResult1>)
			| undefined
			| null,
		onrejected?:
			| ((reason: any) => TResult2 | PromiseLike<TResult2>)
			| undefined
			| null,
	): PromiseLike<TResult1 | TResult2> {
		return (
			tg.Command.new(
				...(this.#args as tg.Args<tg.Command.ArgObject>),
			) as Promise<tg.Command<A, R>>
		).then(onfulfilled, onrejected);
	}

	build(...args: tg.UnresolvedArray<A>): tg.BuildBuilder<[], R> {
		return new tg.BuildBuilder(this as any, { args });
	}

	run(...args: tg.UnresolvedArray<A>): tg.RunBuilder<[], R> {
		return new tg.RunBuilder(this as any, { args });
	}
}
