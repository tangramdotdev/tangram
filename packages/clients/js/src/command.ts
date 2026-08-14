import * as tg from "./index.ts";
import { Resolve } from "./resolve.ts";

/** Create a command. */
export function command<
	A extends tg.UnresolvedArgs<Array<tg.Value>>,
	O extends tg.ReturnValue,
>(
	function_: (...args: A) => O,
): tg.Command.Builder<[], tg.ResolvedReturnValue<O>>;
export function command<
	A extends tg.UnresolvedArgs<Array<tg.Value>>,
	O extends tg.ReturnValue,
>(
	function_: (...args: A) => O,
	...args: tg.UnresolvedArgs<tg.ResolvedArgs<A>>
): tg.Command.Builder<[], tg.ResolvedReturnValue<O>>;
export function command(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<tg.Template.Arg>
): tg.Command.Builder;
export function command(...args: tg.Args<tg.Command.Arg>): tg.Command.Builder;
export function command(...args: any): any {
	if (typeof args[0] === "function") {
		let command = tg.Command.js(args[0], args.slice(1)).then(
			(referent) => referent.node,
		);
		return new tg.Command.Builder(command);
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
		return new tg.Command.Builder(arg);
	} else {
		return new tg.Command.Builder(...args);
	}
}

/** A command. */
export class Command<
	A extends Array<tg.Value> = Array<tg.Value>,
	O extends tg.Value = tg.Value,
> {
	[Resolve.atomic]: null;
	#state: tg.Object.State;

	/** Create a command. */
	static async new<
		A extends Array<tg.Value> = Array<tg.Value>,
		O extends tg.Value = tg.Value,
	>(...args: tg.Args<tg.Command.Arg>): Promise<tg.Command<A, O>> {
		let arg = await tg.Command.arg(...args);
		let args_ = arg.args ?? [];
		let env = arg.env ?? {};
		let executable: tg.Command.Executable | undefined;
		if (tg.Artifact.is(arg.executable)) {
			executable = { artifact: arg.executable, path: null };
		} else if (typeof arg.executable === "string") {
			executable = { artifact: null, path: arg.executable };
		} else if (arg.executable !== undefined && arg.executable !== null) {
			executable = {
				artifact: arg.executable.artifact ?? null,
				path: arg.executable.path ?? null,
			};
		}
		let host = arg.host ?? tg.host.current;
		if (executable === undefined) {
			throw new Error("cannot create a command without an executable");
		}
		if (host === undefined) {
			throw new Error("cannot create a command without a host");
		}
		let stdin =
			arg.stdin === undefined || arg.stdin === null
				? null
				: await tg.blob(arg.stdin);
		let object: tg.Command.Object = {
			args: args_,
			cwd: arg.cwd ?? null,
			env,
			executable,
			host,
			stdin,
			user: arg.user ?? null,
		};
		return tg.Command.withObject(object) as tg.Command<A, O>;
	}

	static async arg(
		...args: tg.Args<tg.Command.Arg>
	): Promise<tg.Command.ResolvedArg> {
		return await tg.Args.apply<
			tg.Command.Arg,
			tg.Command.MappedArg,
			tg.Command.ResolvedArg
		>({
			args,
			map: async (arg): Promise<tg.Command.MappedArg> => {
				let output: tg.ValueOrMaybeMutationMap<tg.Command.Arg>;
				if (arg === undefined) {
					output = {};
				} else if (
					typeof arg === "string" ||
					tg.Artifact.is(arg) ||
					arg instanceof tg.Template
				) {
					let host = tg.host.current;
					output = {
						args: ["-c", arg],
						executable: "sh",
						host,
					};
				} else if (arg instanceof tg.Command) {
					output = await arg.object();
				} else {
					output = arg;
				}
				return {
					...output,
					...(output.args === undefined || output.args === null
						? {}
						: {
								args: output.args.map(tg.Command.Arg.Value.toValue),
							}),
				} as tg.Command.MappedArg;
			},
			reduce: {
				args: (a, b) => [...(a ?? []), ...(b ?? [])],
				env: tg.Command.Arg.Env.reduce,
			},
		});
	}

	static async js(
		function_: Function,
		args: Array<tg.Unresolved<tg.Command.Arg.Value>>,
	): Promise<tg.Referent<tg.Command>> {
		let args_ = await Promise.all(args.map(tg.resolve));
		let target = tg.host.magic(function_);
		let module = tg.Module.fromData(target.module);
		let options = { ...module.referent.options };
		let {
			id: _id,
			name: _name,
			path: _path,
			tag: _tag,
			...rest
		} = module.referent.options ?? {};
		module.referent.options = rest;
		let commandArgs = [
			tg.Command.Value.string("js"),
			...(target.export === undefined || target.export === null
				? []
				: [
						tg.Command.Value.string("--export"),
						tg.Command.Value.string(target.export),
					]),
			tg.Command.Value.string("--host"),
			tg.Command.Value.string(tg.host.current),
			tg.Command.Value.value(module),
		];
		for (let arg of args_) {
			let arg_ =
				arg instanceof tg.Command.Value ? arg : tg.Command.Value.value(arg);
			commandArgs.push(
				tg.Command.Value.string(arg_.kind === "string" ? "-a" : "-A"),
				arg_,
			);
		}
		let command = await tg.Command.new({
			args: commandArgs,
			executable: "tg",
			host: tg.host.current,
		});
		return { node: command, options };
	}

	constructor(arg: tg.Command.ConstructorArg) {
		this[Resolve.atomic] = null;
		let object =
			arg.object !== undefined
				? { kind: "command" as const, value: arg.object }
				: undefined;
		this.#state = new tg.Object.State({
			...(arg.id !== undefined ? { id: arg.id } : {}),
			...(object !== undefined ? { object } : {}),
			stored: arg.stored,
			...(arg.tokens !== undefined ? { tokens: arg.tokens } : {}),
		});
	}

	get state(): tg.Object.State {
		return this.#state;
	}

	/** Get a command with a referent. */
	static withReferent(referent: tg.Referent<tg.Command.Id>): tg.Command {
		let command = tg.Command.withId(referent.node);
		command.state.location = referent.options?.location ?? null;
		command.state.tokens = referent.options?.tokens ?? {};
		return command;
	}

	/** Get a command with an ID. */
	static withId(id: tg.Command.Id): tg.Command {
		return new tg.Command({ id, stored: true });
	}

	static withObject(object: tg.Command.Object): tg.Command {
		return new tg.Command({ object, stored: false });
	}

	static fromData(data: tg.Command.Data): tg.Command {
		return tg.Command.withObject(tg.Command.Object.fromData(data));
	}

	/** Expect that a value is a `tg.Command`. */
	static expect(value: unknown): tg.Command {
		tg.assert(value instanceof tg.Command);
		return value;
	}

	/** Assert that a value is a `tg.Command`. */
	static assert(value: unknown): asserts value is tg.Command {
		tg.assert(value instanceof tg.Command);
	}

	/** Get this command's ID. */
	get id(): tg.Command.Id {
		let id = this.#state.id;
		tg.assert(tg.Object.Id.kind(id) === "command");
		return id;
	}

	/** Get this command's object. */
	async object(): Promise<tg.Command.Object> {
		let object = await this.#state.load();
		tg.assert(object.kind === "command");
		return object.value;
	}

	async load(): Promise<tg.Command.Object> {
		let object = await this.#state.load();
		tg.assert(object.kind === "command");
		return object.value;
	}

	unload(): void {
		this.#state.unload();
	}

	/** Store this command. */
	async store(): Promise<tg.Command.Id> {
		await tg.Value.store(this);
		return this.id;
	}

	get children(): Promise<Array<tg.Object>> {
		return this.#state.children;
	}

	/** Get this command's arguments. */
	get args(): Promise<Array<tg.Command.Value>> {
		return (async () => {
			return (await this.object()).args;
		})();
	}

	/** Get this command's cwd. */
	get cwd(): Promise<string | null> {
		return (async () => {
			return (await this.object()).cwd ?? null;
		})();
	}

	/** Get this command's environment. */
	get env(): Promise<{ [key: string]: tg.Command.Value }> {
		return (async () => {
			return (await this.object()).env;
		})();
	}

	/** Get this command's executable. */
	get executable(): Promise<tg.Command.Executable> {
		return (async () => {
			return (await this.object()).executable;
		})();
	}

	/** Get this command's host. */
	get host(): Promise<string> {
		return (async () => {
			return (await this.object()).host;
		})();
	}

	get stdin(): Promise<tg.Blob | null> {
		return (async () => {
			return (await this.object()).stdin ?? null;
		})();
	}

	/** Get this command's user. */
	get user(): Promise<string | null> {
		return (async () => {
			return (await this.object()).user ?? null;
		})();
	}

	/** Build this command and return the process's output. */
	build(...args: tg.UnresolvedArgs<A>): tg.Process.Builder<"run", [], O> {
		return tg.build(this, { args }) as tg.Process.Builder<"run", [], O>;
	}

	/** Run this command and return the process's output. */
	run(...args: tg.UnresolvedArgs<A>): tg.Process.Builder<"run", [], O> {
		return tg.run(this, { args }) as tg.Process.Builder<"run", [], O>;
	}

	/** Spawn this command and return the process. */
	spawn(...args: tg.UnresolvedArgs<A>): tg.Process.Builder<"spawn", [], O> {
		return tg.spawn(this, { args }) as tg.Process.Builder<"spawn", [], O>;
	}

	/** Exec this command. */
	exec(...args: tg.UnresolvedArgs<A>): tg.Process.Builder<"exec", [], never> {
		return tg.exec(this, { args }) as tg.Process.Builder<"exec", [], never>;
	}
}

export namespace Command {
	export type Id = string;
	export type ConstructorArg = {
		id?: tg.Command.Id;
		object?: tg.Command.Object;
		stored: boolean;
		tokens?: tg.Authorization.Tokens | null;
	};

	export class Value {
		[Resolve.atomic]: null;
		kind: "string" | "value";
		value: tg.Value;

		private constructor(kind: "string" | "value", value: tg.Value) {
			this[Resolve.atomic] = null;
			this.kind = kind;
			this.value = value;
		}

		static string(value: tg.Value): tg.Command.Value {
			return new tg.Command.Value("string", value);
		}

		static value(value: tg.Value): tg.Command.Value {
			return new tg.Command.Value("value", value);
		}
	}

	export namespace Value {
		export type Data =
			| { kind: "string"; value: tg.Value.Data }
			| { kind: "value"; value: tg.Value.Data };

		export let toData = (value: tg.Command.Value): tg.Command.Value.Data => {
			return {
				kind: value.kind,
				value: tg.Value.toData(value.value),
			};
		};

		export let fromData = (data: tg.Command.Value.Data): tg.Command.Value => {
			let value = tg.Value.fromData(data.value);
			return data.kind === "string"
				? tg.Command.Value.string(value)
				: tg.Command.Value.value(value);
		};

		export let children = (value: tg.Command.Value): Array<tg.Object> => {
			return tg.Value.objects(value.value);
		};

		export namespace Data {
			export let children = (
				data: tg.Command.Value.Data,
			): Array<tg.Object.Id> => {
				return tg.Value.Data.children(data.value);
			};

			export let withoutLocationAndTokens = (
				data: tg.Command.Value.Data,
			): tg.Command.Value.Data => {
				return {
					...data,
					value: tg.Value.Data.withoutLocationAndTokens(data.value),
				};
			};
		}
	}

	export type Arg =
		| string
		| tg.Artifact
		| tg.Template
		| tg.Command
		| tg.Command.Arg.Object;

	export namespace Arg {
		export type Value = tg.Value | tg.Command.Value;

		export namespace Value {
			export let toValue = (value: tg.Command.Arg.Value): tg.Command.Value => {
				return value instanceof tg.Command.Value
					? value
					: tg.Command.Value.string(value);
			};
		}

		export type Object = {
			/** The command's arguments. */
			args?: Array<tg.Command.Arg.Value> | null;

			/** The command's working directory. */
			cwd?: string | null;

			/** The command's environment. */
			env?: tg.Command.Arg.Env | null;

			/** The command's executable. */
			executable?: tg.Command.Arg.Executable | null;

			/** The command's host. */
			host?: string | null;

			/** The command's stdin. */
			stdin?: tg.Blob.Arg | null;

			/** The command's user. */
			user?: string | null;
		};

		export type Executable =
			| tg.Artifact
			| string
			| tg.Command.Arg.Executable.Object;

		export namespace Executable {
			export type Object = {
				artifact?: tg.Artifact | null;
				path?: string | null;
			};

			export let is = (value: unknown): value is Executable => {
				return (
					tg.Artifact.is(value) ||
					typeof value === "string" ||
					(typeof value === "object" &&
						value !== null &&
						(!("artifact" in value) ||
							value.artifact === undefined ||
							value.artifact === null ||
							tg.Artifact.is(value.artifact)) &&
						(!("path" in value) ||
							value.path === undefined ||
							value.path === null ||
							typeof value.path === "string"))
				);
			};
		}

		export type Env = {
			[key: string]: tg.Command.Arg.Value | tg.Mutation;
		};

		export namespace Env {
			export let reduce = async (
				a: { [key: string]: tg.Command.Value } | null | undefined,
				b: tg.Command.Arg.Env | null | undefined,
			): Promise<{ [key: string]: tg.Command.Value } | null | undefined> => {
				if (b === null || b === undefined) {
					return b;
				}
				let output = { ...a };
				for (let [key, value] of globalThis.Object.entries(b)) {
					if (!(value instanceof tg.Mutation)) {
						output[key] = tg.Command.Arg.Value.toValue(value);
						continue;
					}
					let current = output[key];
					let kind = current?.kind ?? "string";
					let inner = await value.apply(current?.value);
					if (inner === undefined) {
						delete output[key];
					} else {
						let value_ =
							kind === "string"
								? tg.Command.Value.string(inner)
								: tg.Command.Value.value(inner);
						output[key] = value_;
					}
				}
				return output;
			};
		}
	}

	export type MappedArg = Omit<
		tg.ValueOrMaybeMutationMap<tg.Command.Arg.Object>,
		"args" | "env"
	> & {
		args?: Array<tg.Command.Value> | null;
		env?: tg.Command.Arg.Env | null;
	};

	export type ResolvedArg = Omit<tg.Command.Arg.Object, "args" | "env"> & {
		args?: Array<tg.Command.Value> | null;
		env?: { [key: string]: tg.Command.Value } | null;
	};

	export type Object = {
		args: Array<tg.Command.Value>;
		cwd: string | null;
		env: { [key: string]: tg.Command.Value };
		executable: tg.Command.Executable;
		host: string;
		stdin: tg.Blob | null;
		user: string | null;
	};

	export namespace Object {
		export let isJs = (object: tg.Command.Object): boolean => {
			let firstArg = object.args[0];
			return (
				object.executable.artifact === null &&
				object.executable.path === "tg" &&
				firstArg?.kind === "string" &&
				firstArg.value === "js"
			);
		};

		export let toData = (object: tg.Command.Object): tg.Command.Data => {
			let output: Data = {
				args: object.args.map(tg.Command.Value.toData),
				env: globalThis.Object.fromEntries(
					globalThis.Object.entries(object.env).map(([key, value]) => [
						key,
						tg.Command.Value.toData(value),
					]),
				),
				executable: tg.Command.Executable.toData(object.executable),
				host: object.host,
			};
			if (object.cwd !== null) {
				output.cwd = object.cwd;
			}
			if (object.stdin !== null) {
				output.stdin = object.stdin.id;
			}
			if (object.user !== null) {
				output.user = object.user;
			}
			return output;
		};

		export let fromData = (data: tg.Command.Data): tg.Command.Object => {
			let object: tg.Command.Object = {
				args: (data.args ?? []).map(tg.Command.Value.fromData),
				cwd: data.cwd ?? null,
				env: globalThis.Object.fromEntries(
					globalThis.Object.entries(data.env ?? {}).map(([key, value]) => [
						key,
						tg.Command.Value.fromData(value),
					]),
				),
				executable: tg.Command.Executable.fromData(data.executable),
				host: data.host,
				stdin:
					data.stdin === undefined || data.stdin === null
						? null
						: tg.Blob.withId(data.stdin),
				user: data.user ?? null,
			};
			return object;
		};

		export let children = (object: tg.Command.Object): Array<tg.Object> => {
			return [
				...object.args.flatMap(tg.Command.Value.children),
				...globalThis.Object.entries(object.env).flatMap(([_, value]) =>
					tg.Command.Value.children(value),
				),
				...tg.Command.Executable.children(object.executable),
				...(object.stdin !== null ? [object.stdin] : []),
			];
		};
	}

	export type Executable = {
		artifact: tg.Artifact | null;
		path: string | null;
	};

	export namespace Executable {
		export let toData = (
			value: tg.Command.Executable,
		): tg.Command.Data.Executable => {
			let output: tg.Command.Data.Executable = {};
			if (value.artifact !== null) {
				output.artifact = value.artifact.id;
			}
			if (value.path !== null) {
				output.path = value.path;
			}
			return output;
		};

		export let fromData = (
			data: tg.Command.Data.Executable,
		): tg.Command.Executable => {
			return {
				artifact:
					data.artifact === undefined || data.artifact === null
						? null
						: tg.Artifact.withId(data.artifact),
				path: data.path ?? null,
			};
		};

		export let children = (value: tg.Command.Executable): Array<tg.Object> => {
			return value.artifact === null ? [] : [value.artifact];
		};
	}

	export type Data = {
		args?: Array<tg.Command.Value.Data>;
		cwd?: string | null;
		env?: { [key: string]: tg.Command.Value.Data };
		executable: tg.Command.Data.Executable;
		host: string;
		stdin?: tg.Blob.Id | null;
		user?: string | null;
	};

	export namespace Data {
		export let children = (data: tg.Command.Data): Array<tg.Object.Id> => {
			return [
				...tg.Command.Data.Executable.children(data.executable),
				...(data.args ?? []).flatMap(tg.Command.Value.Data.children),
				...globalThis.Object.values(data.env ?? {}).flatMap(
					tg.Command.Value.Data.children,
				),
			];
		};

		export let withoutLocationAndTokens = (
			data: tg.Command.Data,
		): tg.Command.Data => {
			let output = { ...data };
			if (data.args !== undefined) {
				output.args = data.args.map(
					tg.Command.Value.Data.withoutLocationAndTokens,
				);
			}
			if (data.env !== undefined) {
				output.env = globalThis.Object.fromEntries(
					globalThis.Object.entries(data.env).map(([key, value]) => [
						key,
						tg.Command.Value.Data.withoutLocationAndTokens(value),
					]),
				);
			}
			output.executable = tg.Command.Data.Executable.withoutLocationAndTokens(
				data.executable,
			);
			return output;
		};

		export type Executable = {
			artifact?: tg.Artifact.Id | null;
			path?: string | null;
		};

		export namespace Executable {
			export let children = (
				data: tg.Command.Data.Executable,
			): Array<tg.Object.Id> => {
				return data.artifact === undefined || data.artifact === null
					? []
					: [data.artifact];
			};

			export let withoutLocationAndTokens = (
				data: tg.Command.Data.Executable,
			): tg.Command.Data.Executable => {
				return { ...data };
			};
		}
	}
}

export namespace Command {
	export interface Builder<
		A extends Array<tg.Value> = Array<tg.Value>,
		O extends tg.Value = tg.Value,
		E = tg.Command.Arg.Env,
	> {
		(
			...args: { [K in keyof A]: tg.Unresolved<A[K]> }
		): tg.Command.Builder<[], O, E>;
	}

	export class Builder<
		A extends Array<tg.Value> = Array<tg.Value>,
		O extends tg.Value = tg.Value,
		E = tg.Command.Arg.Env,
	> extends Function {
		#args: tg.Args<tg.Command.Arg.Object>;
		#envMapper: tg.Command.Builder.EnvMapper<E>;
		#js: Promise<boolean>;

		constructor(...args: tg.Args<tg.Command.Arg.Object>) {
			super();
			this.#envMapper = ((env: tg.Command.Arg.Env) =>
				env) as tg.Command.Builder.EnvMapper<E>;
			this.#js = isJsCommandBuilderArg(args);
			this.#args = args.map((arg) => this.builderArg(arg));
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
					return globalThis.Object.getPrototypeOf(this_);
				},
			});
		}

		arg(...args: Array<tg.Unresolved<tg.Command.Arg.Value>>): this {
			return this.args(args);
		}

		args(
			...args: Array<tg.Unresolved<Array<tg.Command.Arg.Value> | null>>
		): this {
			this.#args.push(...args.map((args) => this.argsArg(args)));
			return this;
		}

		cwd(cwd: tg.Unresolved<tg.MaybeMutation<string> | null>): this {
			this.#args.push({ cwd });
			return this;
		}

		env(...envs: Array<tg.Unresolved<E | null>>): this {
			this.#args.push(...envs.map((env) => this.envArg(env)));
			return this;
		}

		envMapper<E_>(
			envMapper: tg.Command.Builder.EnvMapper<E_>,
		): tg.Command.Builder<A, O, E_> {
			let builder = this as unknown as tg.Command.Builder<A, O, E_>;
			builder.#envMapper = envMapper;
			return builder;
		}

		executable(
			executable: tg.Unresolved<tg.MaybeMutation<tg.Command.Arg.Executable> | null>,
		): this {
			this.#args.push({ executable });
			return this;
		}

		host(host: tg.Unresolved<tg.MaybeMutation<string> | null>): this {
			this.#args.push({ host });
			return this;
		}

		/** Build this command and return the process's output. */
		build(...args: tg.UnresolvedArgs<A>): tg.Process.Builder<"run", [], O, E> {
			return tg
				.build(...this.#args, this.argsArg(args))
				.envMapper<E>(this.#envMapper) as tg.Process.Builder<"run", [], O, E>;
		}

		/** Run this command and return the process's output. */
		run(...args: tg.UnresolvedArgs<A>): tg.Process.Builder<"run", [], O, E> {
			return tg
				.run(...this.#args, this.argsArg(args))
				.envMapper<E>(this.#envMapper) as tg.Process.Builder<"run", [], O, E>;
		}

		/** Spawn this command and return the process. */
		spawn(
			...args: tg.UnresolvedArgs<A>
		): tg.Process.Builder<"spawn", [], O, E> {
			return tg
				.spawn(...this.#args, this.argsArg(args))
				.envMapper<E>(this.#envMapper) as tg.Process.Builder<"spawn", [], O, E>;
		}

		/** Exec this command. */
		exec(
			...args: tg.UnresolvedArgs<A>
		): tg.Process.Builder<"exec", [], never, E> {
			return tg
				.exec(...this.#args, this.argsArg(args))
				.envMapper<E>(this.#envMapper) as tg.Process.Builder<
				"exec",
				[],
				never,
				E
			>;
		}

		then<TResult1 = tg.Command<A, O>, TResult2 = never>(
			onfulfilled?:
				| ((value: tg.Command<A, O>) => TResult1 | PromiseLike<TResult1>)
				| undefined
				| null,
			onrejected?:
				| ((reason: any) => TResult2 | PromiseLike<TResult2>)
				| undefined
				| null,
		): PromiseLike<TResult1 | TResult2> {
			return tg.Command.new(...this.#args)
				.then((command) => command as tg.Command<A, O>)
				.then(onfulfilled, onrejected);
		}

		private async builderArg(
			arg: tg.Unresolved<tg.ValueOrMaybeMutationMap<tg.Command.Arg.Object>>,
		): Promise<tg.ValueOrMaybeMutationMap<tg.Command.Arg.Object>> {
			let [js, arg_] = await Promise.all([this.#js, tg.resolve(arg)]);
			if (
				!js ||
				arg_ instanceof tg.Command ||
				typeof arg_ !== "object" ||
				arg_ === null ||
				!("args" in arg_) ||
				!Array.isArray(arg_.args)
			) {
				return arg_;
			}
			let args = encodeJsArgs(arg_.args);

			return { ...arg_, args };
		}

		private async argsArg(
			args: tg.Unresolved<Array<tg.Command.Arg.Value> | null>,
		): Promise<tg.Command.Arg.Object> {
			let [js, args_] = await Promise.all([this.#js, tg.resolve(args)]);
			if (!js || args_ === null) {
				return { args: args_ };
			}
			let output = encodeJsArgs(args_);

			return { args: output };
		}

		private envArg(
			env: tg.Unresolved<E | null>,
		): tg.Unresolved<tg.Command.Arg.Object> {
			let envMapper = this.#envMapper;
			return tg.resolve(env).then(async (env) => {
				if (env === null) {
					return { env: null };
				}
				let output = envMapper(env as E);
				return { env: await tg.resolve(output) };
			});
		}
	}

	export namespace Builder {
		export type EnvMapper<E> = (env: E) => tg.Unresolved<tg.Command.Arg.Env>;
	}
}

async function isJsCommandBuilderArg(
	args: tg.Args<tg.Command.Arg.Object>,
): Promise<boolean> {
	let args_ = await Promise.all(args.map(tg.resolve));
	for (let arg of args_) {
		if (
			arg instanceof tg.Command &&
			tg.Command.Object.isJs(await arg.object())
		) {
			return true;
		}
	}

	return false;
}

function encodeJsArgs(
	args: Array<tg.Command.Arg.Value>,
): Array<tg.Command.Value> {
	let encoded =
		args.length % 2 === 0 &&
		args.every((value, index) => {
			if (index % 2 !== 0) {
				return value instanceof tg.Command.Value;
			}
			let next = args[index + 1];
			return (
				value instanceof tg.Command.Value &&
				value.kind === "string" &&
				(value.value === "-a" || value.value === "-A") &&
				next instanceof tg.Command.Value &&
				value.value === (next.kind === "string" ? "-a" : "-A")
			);
		});
	if (encoded) {
		return args as Array<tg.Command.Value>;
	}
	return args.flatMap((value) => {
		let value_ =
			value instanceof tg.Command.Value ? value : tg.Command.Value.value(value);
		return [
			tg.Command.Value.string(value_.kind === "string" ? "-a" : "-A"),
			value_,
		];
	});
}
