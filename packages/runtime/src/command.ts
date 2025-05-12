import * as tg from "./index.ts";

export function command<
	A extends tg.UnresolvedArgs<Array<tg.Value>>,
	R extends tg.ReturnValue,
>(function_: (...args: A) => R): tg.RunBuilder<[], tg.ResolvedReturnValue<R>>;
export function command<
	A extends tg.UnresolvedArgs<Array<tg.Value>>,
	R extends tg.ReturnValue,
>(
	function_: (...args: A) => R,
	...args: tg.UnresolvedArgs<tg.ResolvedArgs<A>>
): tg.RunBuilder<[], tg.ResolvedReturnValue<R>>;
export function command(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<tg.Template.Arg>
): tg.CommandBuilder;
export function command(...args: tg.Args<tg.Command.Arg>): tg.CommandBuilder;
export function command(...args: any): any {
	if (typeof args[0] === "function") {
		return new CommandBuilder({
			host: "js",
			executable: tg.Command.Executable.fromData(syscall("magic", args[0])),
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
		return new Command({ id, stored: true });
	}

	static withObject(object: Command.Object): Command {
		return new Command({ object, stored: false });
	}

	static fromData(data: Command.Data): Command {
		return Command.withObject(Command.Object.fromData(data));
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
				export: arg.executable.export,
			};
		} else if (arg.executable !== undefined && "path" in arg.executable) {
			executable = {
				path: arg.executable.path,
			};
		}
		let host =
			arg.host ?? ((await tg.Process.current.env("TANGRAM_HOST")) as string);
		let mounts: Array<tg.Command.Mount> = [];
		if (arg.mounts && arg.mounts.length > 0) {
			mounts = arg.mounts.map((mount) => {
				if (typeof mount === "string" || mount instanceof tg.Template) {
					return tg.Command.Mount.parse(mount);
				} else {
					return mount;
				}
			});
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
		return Command.withObject(object) as Command<A, R>;
	}

	static async arg(...args: tg.Args<Command.Arg>): Promise<Command.ArgObject> {
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
			},
			reduce: {
				args: "append",
				env: "merge",
			},
		});
	}

	static expect(value: unknown): Command {
		tg.assert(value instanceof Command);
		return value;
	}

	static assert(value: unknown): asserts value is Command {
		tg.assert(value instanceof Command);
	}

	get id(): Command.Id {
		if (this.#state.id! !== undefined) {
			return this.#state.id;
		}
		let object = this.#state.object!;
		let data = Command.Object.toData(object);
		let id = syscall("object_id", { kind: "command", value: data });
		this.#state.id = id;
		return id;
	}

	async object(): Promise<Command.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load(): Promise<tg.Command.Object> {
		if (this.#state.object === undefined) {
			let data = await syscall("object_get", this.#state.id!);
			tg.assert(data.kind === "command");
			let object = Command.Object.fromData(data.value);
			this.#state.object = object;
		}
		return this.#state.object!;
	}

	async store(): Promise<tg.Command.Id> {
		await tg.Value.store(this);
		return this.id;
	}

	async children(): Promise<Array<tg.Object>> {
		let object = await this.object();
		return tg.Command.Object.children(object);
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

	build(...args: tg.UnresolvedArgs<A>): tg.BuildBuilder<[], R> {
		return tg.build(this, { args }) as tg.BuildBuilder<[], R>;
	}

	run(...args: tg.UnresolvedArgs<A>): tg.RunBuilder<[], R> {
		return tg.run(this, { args }) as tg.RunBuilder<[], R>;
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
		mounts?: Array<string | tg.Template | tg.Command.Mount> | undefined;
		stdin?: tg.Blob.Arg | undefined;
		user?: string | undefined;
	};

	export type Executable =
		| tg.Command.Executable.Artifact
		| tg.Command.Executable.Module
		| tg.Command.Executable.Path;

	export namespace Executable {
		export let toData = (value: Executable): ExecutableData => {
			if ("artifact" in value) {
				let output: ExecutableData = {
					artifact: value.artifact.id,
				};
				if (value.subpath !== undefined) {
					output.subpath = value.subpath;
				}
				return output;
			} else if ("module" in value) {
				let output: ExecutableData = {
					module: tg.Module.toData(value.module),
				};
				if (value.export !== undefined) {
					output.export = value.export;
				}
				return output;
			} else if ("path" in value) {
				return {
					path: value.path,
				};
			} else {
				throw new Error("invalid executable");
			}
		};

		export let fromData = (data: ExecutableData): Executable => {
			if ("artifact" in data) {
				return {
					artifact: tg.Artifact.withId(data.artifact),
					subpath: data.subpath,
				};
			} else if ("module" in data) {
				return {
					module: tg.Module.fromData(data.module),
					export: data.export,
				};
			} else if ("path" in data) {
				return {
					path: data.path,
				};
			} else {
				throw new Error("invalid executable");
			}
		};

		export let children = (value: Executable): Array<tg.Object> => {
			if ("artifact" in value) {
				return [value.artifact];
			} else if ("module" in value) {
				return tg.Module.children(value.module);
			} else {
				return [];
			}
		};
	}

	export type ExecutableData =
		| tg.Command.Executable.ArtifactData
		| tg.Command.Executable.ModuleData
		| tg.Command.Executable.PathData;

	export type ExecutableArg =
		| tg.Artifact
		| string
		| tg.Command.Executable.ArtifactArg
		| tg.Command.Executable.ModuleArg
		| tg.Command.Executable.PathArg;

	export namespace Executable {
		export type ArtifactArg = {
			artifact: tg.Artifact;
			subpath?: string | undefined;
		};

		export type Artifact = {
			artifact: tg.Artifact;
			subpath: string | undefined;
		};

		export type ArtifactData = {
			artifact: tg.Artifact.Id;
			subpath?: string;
		};

		export type ModuleArg = {
			module: tg.Module;
			export?: string | undefined;
		};

		export type Module = {
			module: tg.Module;
			export: string | undefined;
		};

		export type ModuleData = {
			module: tg.Module.Data;
			export?: string;
		};

		export type PathArg = {
			path: string;
		};

		export type Path = {
			path: string;
		};

		export type PathData = {
			path: string;
		};
	}

	export type Id = string;

	export type Mount = {
		source: tg.Artifact;
		target: string;
	};

	export type MountData = {
		source: tg.Artifact.Id;
		target: string;
	};

	export namespace Mount {
		export let toData = (data: Mount): MountData => {
			return {
				source: data.source.id,
				target: data.target,
			};
		};

		export let fromData = (data: MountData): Mount => {
			return {
				source: tg.Artifact.withId(data.source),
				target: data.target,
			};
		};

		export let parse = (t: string | tg.Template): tg.Command.Mount => {
			// If the user passed a template, render a string with artifact IDs.
			let s: string | undefined;
			if (typeof t === "string") {
				s = t;
			} else if (t instanceof tg.Template) {
				s = t.components.reduce<string>((s, component) => {
					if (tg.Artifact.is(component)) {
						return s + component.id;
					} else {
						return s + component;
					}
				}, "");
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
		mounts: Array<tg.Command.Mount>;
		stdin: tg.Blob | undefined;
		user: string | undefined;
	};

	export namespace Object {
		export let toData = (object: Object): Data => {
			let output: Data = {
				args: object.args.map(tg.Value.toData),
				env: globalThis.Object.fromEntries(
					globalThis.Object.entries(object.env).map(([key, value]) => [
						key,
						tg.Value.toData(value),
					]),
				),
				executable: tg.Command.Executable.toData(object.executable),
				host: object.host,
			};
			if (object.cwd !== undefined) {
				output.cwd = object.cwd;
			}
			if (object.mounts.length > 0) {
				output.mounts = object.mounts.map(tg.Command.Mount.toData);
			}
			if (object.stdin !== undefined) {
				output.stdin = object.stdin.id;
			}
			if (object.user !== undefined) {
				output.user = object.user;
			}
			return output;
		};

		export let fromData = (data: Data): Object => {
			return {
				args: (data.args ?? []).map(tg.Value.fromData),
				cwd: data.cwd,
				env: globalThis.Object.fromEntries(
					globalThis.Object.entries(data.env ?? {}).map(([key, value]) => [
						key,
						tg.Value.fromData(value),
					]),
				),
				executable: tg.Command.Executable.fromData(data.executable),
				host: data.host,
				mounts: (data.mounts ?? []).map(tg.Command.Mount.fromData),
				stdin:
					data.stdin !== undefined ? tg.Blob.withId(data.stdin) : undefined,
				user: data.user,
			};
		};

		export let children = (object: Object): Array<tg.Object> => {
			return [
				...object.args.flatMap(tg.Value.objects),
				...globalThis.Object.entries(object.env).flatMap(([_, value]) =>
					tg.Value.objects(value),
				),
				...tg.Command.Executable.children(object.executable),
				...object.mounts.map(({ source }) => source),
				...(object.stdin !== undefined ? [object.stdin] : []),
			];
		};
	}

	export type Data = {
		args?: Array<tg.Value.Data>;
		cwd?: string;
		env?: { [key: string]: tg.Value.Data };
		executable: tg.Command.ExecutableData;
		host: string;
		mounts?: Array<tg.Command.MountData>;
		stdin?: tg.Blob.Id;
		user?: string;
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
	#args: tg.Args<tg.Command.ArgObject>;

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

	arg(...args: Array<tg.Unresolved<tg.Value>>): this {
		this.#args.push({ args });
		return this;
	}

	args(...args: Array<tg.Unresolved<tg.MaybeMutation<Array<tg.Value>>>>): this {
		this.#args.push(...args.map((args) => ({ args })));
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
		...mounts: Array<tg.Unresolved<string | tg.Template | tg.Command.Mount>>
	): this {
		this.#args.push({ mounts });
		return this;
	}

	mounts(
		...mounts: Array<
			tg.Unresolved<
				tg.MaybeMutation<Array<string | tg.Template | tg.Command.Mount>>
			>
		>
	): this {
		this.#args.push(...mounts.map((mounts) => ({ mounts })));
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
		return tg.Command.new(...this.#args)
			.then((command) => command as tg.Command<A, R>)
			.then(onfulfilled, onrejected);
	}

	build(...args: tg.UnresolvedArgs<A>): tg.BuildBuilder<[], R> {
		return new tg.BuildBuilder(...this.#args, { args });
	}

	run(...args: tg.UnresolvedArgs<A>): tg.RunBuilder<[], R> {
		return new tg.RunBuilder(...this.#args, { args });
	}
}
