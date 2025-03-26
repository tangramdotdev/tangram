import * as tg from "./index.ts";
import {
	type MaybeMutationMap,
	type MaybeNestedArray,
	type MaybePromise,
	flatten,
} from "./util.ts";

type FunctionArg<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
> = {
	function: (...args: A) => tg.Unresolved<R>;
	module: tg.Module;
	name: string;
};

export function command<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
>(arg: FunctionArg): Command<A, R>;
export function command<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
>(...args: tg.Args<Command.Arg>): Promise<Command<A, R>>;
export function command<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
>(
	...args: [FunctionArg<A, R>] | tg.Args<Command.Arg>
): MaybePromise<Command<A, R>> {
	if (
		args.length === 1 &&
		typeof args[0] === "object" &&
		"function" in args[0]
	) {
		let arg = args[0];
		let args_ = [arg.name];
		let executable = {
			kind: arg.module.kind,
			referent: {
				item: tg.Object.withId(arg.module.referent.item),
				path: arg.module.referent.path,
				subpath: arg.module.referent.subpath,
				tag: arg.module.referent.tag,
			},
		};
		let cwd = undefined;
		let mounts: Array<tg.Command.Mount> = [];
		let stdin = undefined;
		let user = undefined;
		let object = {
			args: args_,
			cwd,
			env: tg.Process.current.state!.command.state.object!.env,
			executable,
			host: "js",
			mounts,
			stdin,
			user,
		};
		let state = { object };
		return new Command(state, arg.function);
	} else {
		return Command.new(...(args as tg.Args<Command.Arg>));
	}
}

export interface Command<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
> extends globalThis.Function {
	(...args: { [K in keyof A]: tg.Unresolved<A[K]> }): Promise<R>;
}

// biome-ignore lint/suspicious/noUnsafeDeclarationMerging: This is necessary to make commands callable.
export class Command<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
> extends globalThis.Function {
	#state: Command.State;
	#f: Function | undefined;

	constructor(state: Command.State, f?: Function) {
		super();
		this.#state = state;
		this.#f = f;
		let this_ = this as any;
		// biome-ignore lint/correctness/noConstructorReturn: This is necessary to make commands callable.
		return new Proxy(this_, {
			get(_command, prop, _receiver) {
				if (typeof this_[prop] === "function") {
					return this_[prop].bind(this_);
				} else {
					return this_[prop];
				}
			},
			apply: async (command, _, args) => {
				return await tg.run(command, { args });
			},
			getPrototypeOf: (_command) => {
				return Object.getPrototypeOf(this_);
			},
		});
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
		let env = await tg.Args.applyMutations(flatten(arg.env ?? []));
		let executable = arg.executable;
		let host = arg.host;
		let mounts = await Promise.all(
			(arg.mounts ?? []).map(async (mount) => {
				if (mount instanceof tg.Template || typeof mount === "string") {
					return await tg.Command.Mount.parse(mount);
				} else {
					return mount;
				}
			}),
		);
		if (!host) {
			throw new Error("cannot create a command without a host");
		}
		let stdin = undefined;
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
		let flattened = flatten(resolved);
		let objects = await Promise.all(
			flattened.map(async (arg) => {
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
		let mutations = await tg.Args.createMutations(objects, {
			args: "append",
			env: "append",
		});
		let arg = await tg.Args.applyMutations(mutations);
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

	async env(): Promise<{ [key: string]: tg.Value }> {
		return (await this.object()).env;
	}

	async executable(): Promise<tg.Command.Executable | undefined> {
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

	async mounts(): Promise<Array<tg.Command.Mount>> {
		return (await this.object()).mounts;
	}

	function(): Function | undefined {
		return this.#f;
	}

	async build(...args: tg.Args<tg.Process.RunArg>): Promise<tg.Value> {
		return await tg.Process.build(
			this as Command<Array<tg.Value>, tg.Value>,
			...args,
		);
	}

	async run(...args: tg.Args<tg.Process.RunArg>): Promise<tg.Value> {
		return await tg.Process.run(
			this as Command<Array<tg.Value>, tg.Value>,
			...args,
		);
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
		env?: MaybeNestedArray<MaybeMutationMap> | undefined;
		executable?: tg.Command.ExecutableArg | undefined;
		host?: string | undefined;
		mounts?: Array<string | tg.Template | tg.Command.Mount> | undefined;
		stdin?: tg.Blob.Id | undefined;
		user?: string | undefined;
	};

	export type Executable = string | tg.Artifact | tg.Command.Executable.Module;

	export namespace Executable {
		export type Module = {
			kind: tg.Module.Kind;
			referent: tg.Referent<tg.Object>;
		};
	}

	export type ExecutableArg =
		| string
		| tg.Artifact
		| tg.Command.Executable.Module;

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
		executable: tg.Command.Executable | undefined;
		host: string;
		mounts: Array<tg.Command.Mount>;
		stdin: tg.Blob | undefined;
		user: string | undefined;
	};

	export type State = tg.Object.State<Command.Id, Command.Object>;
}
