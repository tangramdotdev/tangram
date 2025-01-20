import * as tg from "./index.ts";
import {
	type MaybeMutationMap,
	type MaybeNestedArray,
	type MaybePromise,
	flatten,
} from "./util.ts";

let currentCommand: Command;

export let setCurrentCommand = (command: Command) => {
	currentCommand = command;
};

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

		// Create the command.
		let args_ = [arg.name];
		let checksum = undefined;
		let cwd = undefined;
		let executable = {
			kind: arg.module.kind,
			referent: {
				item: tg.Object.withId(arg.module.referent.item),
				path: arg.module.referent.path,
				subpath: arg.module.referent.subpath,
				tag: arg.module.referent.tag,
			},
		};
		let network = false;
		const env = currentCommand.state.object!.env;
		let object = {
			args: args_,
			checksum,
			cwd,
			env,
			executable,
			host: "js",
			network,
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
				return await (await Command.new(command, { args })).output();
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
		let checksum = arg.checksum;
		let cwd = arg.cwd;
		let env = await tg.Args.applyMutations(flatten(arg.env ?? []));
		let executable = arg.executable;
		let host = arg.host;
		let network = arg.network ?? false;
		if (!host) {
			throw new Error("cannot create a command without a host");
		}
		let object = {
			args: args_,
			checksum,
			cwd,
			env,
			executable,
			host,
			network,
		};
		return new Command({ object });
	}

	static get current(): Command {
		return currentCommand;
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
					return {
						args: ["-c", arg],
						executable: await tg.symlink("/bin/sh"),
						host: (await currentCommand.env()).TANGRAM_HOST as string,
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

	async checksum(): Promise<tg.Checksum | undefined> {
		return (await this.object()).checksum;
	}

	async cwd(): Promise<string | undefined> {
		return (await this.object()).cwd;
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

	async network(): Promise<boolean> {
		return (await this.object()).network;
	}

	async output(): Promise<R> {
		return (await syscall("command_output", this as Command<[], R>)) as R;
	}

	function(): Function | undefined {
		return this.#f;
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
		checksum?: tg.Checksum | undefined;
		cwd?: string | undefined;
		env?: MaybeNestedArray<MaybeMutationMap> | undefined;
		executable?: tg.Command.ExecutableArg | undefined;
		host?: string | undefined;
		network?: boolean | undefined;
	};

	export type Executable = tg.Artifact | tg.Command.Executable.Module;

	export namespace Executable {
		export type Module = {
			kind: tg.Module.Kind;
			referent: tg.Referent<tg.Object>;
		};
	}

	export type ExecutableArg = tg.Artifact | tg.Command.Executable.Module;

	export type Id = string;

	export type Object = {
		args: Array<tg.Value>;
		checksum: tg.Checksum | undefined;
		cwd: string | undefined;
		env: { [key: string]: tg.Value };
		executable: tg.Command.Executable | undefined;
		host: string;
		network: boolean;
	};

	export type State = tg.Object.State<Command.Id, Command.Object>;
}
