import * as tg from "./index.ts";

export function command<
	A extends tg.UnresolvedArgs<Array<tg.Value>>,
	R extends tg.ReturnValue,
>(
	function_: (...args: A) => R,
): tg.CommandBuilder<[], tg.ResolvedReturnValue<R>>;
export function command<
	A extends tg.UnresolvedArgs<Array<tg.Value>>,
	R extends tg.ReturnValue,
>(
	function_: (...args: A) => R,
	...args: tg.UnresolvedArgs<tg.ResolvedArgs<A>>
): tg.CommandBuilder<[], tg.ResolvedReturnValue<R>>;
export function command(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<tg.Template.Arg>
): tg.CommandBuilder;
export function command(...args: tg.Args<tg.Command.Arg>): tg.CommandBuilder;
export function command(...args: any): any {
	if (typeof args[0] === "function") {
		return new CommandBuilder({
			host: "js",
			executable: tg.Command.Executable.fromData(tg.handle.magic(args[0])),
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
	#state: tg.Command.State;

	constructor(state: tg.Command.State) {
		this.#state = state;
	}

	get state(): tg.Command.State {
		return this.#state;
	}

	static withId(id: tg.Command.Id): tg.Command {
		return new tg.Command({ id, stored: true });
	}

	static withObject(object: tg.Command.Object): tg.Command {
		return new tg.Command({ object, stored: false });
	}

	static fromData(data: tg.Command.Data): tg.Command {
		return tg.Command.withObject(Command.Object.fromData(data));
	}

	static async new<
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	>(...args: tg.Args<tg.Command.Arg>): Promise<tg.Command<A, R>> {
		let arg = await tg.Command.arg(...args);
		let args_ = arg.args ?? [];
		let cwd = arg.cwd;
		let env = arg.env ?? {};
		let executable: tg.Command.Executable | undefined;
		if (tg.Artifact.is(arg.executable)) {
			executable = { artifact: arg.executable, path: undefined };
		} else if (typeof arg.executable === "string") {
			executable = { path: arg.executable };
		} else if (arg.executable !== undefined && "artifact" in arg.executable) {
			executable = {
				artifact: arg.executable.artifact,
				path: arg.executable.path,
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
			mounts = arg.mounts;
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
		return tg.Command.withObject(object) as tg.Command<A, R>;
	}

	static async arg(
		...args: tg.Args<tg.Command.Arg>
	): Promise<tg.Command.Arg.Object> {
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
					let host = await tg.Process.current
						.command()
						.then((command) => command.env())
						.then((env) => env.TANGRAM_HOST);
					return {
						args: ["-c", arg],
						executable: "/bin/sh",
						host,
					};
				} else if (arg instanceof tg.Command) {
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

	static expect(value: unknown): tg.Command {
		tg.assert(value instanceof tg.Command);
		return value;
	}

	static assert(value: unknown): asserts value is tg.Command {
		tg.assert(value instanceof tg.Command);
	}

	get id(): tg.Command.Id {
		if (this.#state.id! !== undefined) {
			return this.#state.id;
		}
		let object = this.#state.object!;
		let data = tg.Command.Object.toData(object);
		let id = tg.handle.objectId({ kind: "command", value: data });
		this.#state.id = id;
		return id;
	}

	async object(): Promise<tg.Command.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load(): Promise<tg.Command.Object> {
		if (this.#state.object === undefined) {
			let data = await tg.handle.getObject(this.#state.id!);
			tg.assert(data.kind === "command");
			let object = tg.Command.Object.fromData(data.value);
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
	export type Id = string;

	export type State = tg.Object.State<tg.Command.Id, tg.Command.Object>;

	export type Arg =
		| undefined
		| string
		| tg.Artifact
		| tg.Template
		| tg.Command
		| tg.Command.Arg.Object;

	export namespace Arg {
		export type Object = {
			args?: Array<tg.Value> | undefined;
			cwd?: string | undefined;
			env?: tg.MaybeMutationMap | undefined;
			executable?: tg.Command.Arg.Executable | undefined;
			host?: string | undefined;
			mounts?: Array<tg.Command.Mount> | undefined;
			stdin?: tg.Blob.Arg | undefined;
			user?: string | undefined;
		};

		export type Executable =
			| tg.Artifact
			| string
			| tg.Command.Arg.Executable.Artifact
			| tg.Command.Arg.Executable.Module
			| tg.Command.Arg.Executable.Path;

		export namespace Executable {
			export type Artifact = {
				artifact: tg.Artifact;
				path?: string | undefined;
			};

			export type Module = {
				module: tg.Module;
				export?: string | undefined;
			};

			export type Path = {
				path: string;
			};
		}
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
		export let toData = (object: tg.Command.Object): tg.Command.Data => {
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

		export let fromData = (data: tg.Command.Data): tg.Command.Object => {
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

		export let children = (object: tg.Command.Object): Array<tg.Object> => {
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

	export type Executable =
		| tg.Command.Executable.Artifact
		| tg.Command.Executable.Module
		| tg.Command.Executable.Path;

	export namespace Executable {
		export let toData = (
			value: tg.Command.Executable,
		): tg.Command.Data.Executable => {
			if ("artifact" in value) {
				let output: tg.Command.Data.Executable = {
					artifact: value.artifact.id,
				};
				if (value.path !== undefined) {
					output.path = value.path;
				}
				return output;
			} else if ("module" in value) {
				let output: tg.Command.Data.Executable = {
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

		export let fromData = (
			data: tg.Command.Data.Executable,
		): tg.Command.Executable => {
			if ("artifact" in data) {
				return {
					artifact: tg.Artifact.withId(data.artifact),
					path: data.path,
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

		export let children = (value: tg.Command.Executable): Array<tg.Object> => {
			if ("artifact" in value) {
				return [value.artifact];
			} else if ("module" in value) {
				return tg.Module.children(value.module);
			} else {
				return [];
			}
		};
	}

	export namespace Executable {
		export type Artifact = {
			artifact: tg.Artifact;
			path: string | undefined;
		};

		export type Module = {
			module: tg.Module;
			export: string | undefined;
		};

		export type Path = {
			path: string;
		};
	}

	export type Mount = {
		source: tg.Artifact;
		target: string;
	};

	export namespace Mount {
		export let toData = (data: tg.Command.Mount): tg.Command.Data.Mount => {
			return {
				source: data.source.id,
				target: data.target,
			};
		};

		export let fromData = (data: tg.Command.Data.Mount): tg.Command.Mount => {
			return {
				source: tg.Artifact.withId(data.source),
				target: data.target,
			};
		};
	}

	export type Data = {
		args?: Array<tg.Value.Data>;
		cwd?: string;
		env?: { [key: string]: tg.Value.Data };
		executable: tg.Command.Data.Executable;
		host: string;
		mounts?: Array<tg.Command.Data.Mount>;
		stdin?: tg.Blob.Id;
		user?: string;
	};

	export namespace Data {
		export type Executable =
			| tg.Command.Data.Executable.Artifact
			| tg.Command.Data.Executable.Module
			| tg.Command.Data.Executable.Path;

		export namespace Executable {
			export type Artifact = {
				artifact: tg.Artifact.Id;
				path?: string;
			};

			export type Module = {
				module: tg.Module.Data;
				export?: string;
			};

			export type Path = {
				path: string;
			};
		}

		export type Mount = {
			source: tg.Artifact.Id;
			target: string;
		};
	}
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
	#args: tg.Args<tg.Command.Arg.Object>;

	constructor(...args: tg.Args<tg.Command.Arg.Object>) {
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

	// biome-ignore lint/suspicious/noThenProperty: <reason>
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
