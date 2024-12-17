import * as tg from "./index.ts";
import {
	type MaybeMutationMap,
	type MaybeNestedArray,
	type MaybePromise,
	flatten,
} from "./util.ts";

let currentTarget: Target;

export let setCurrentTarget = (target: Target) => {
	currentTarget = target;
};

type FunctionArg<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
> = {
	function: (...args: A) => tg.Unresolved<R>;
	module: tg.Module;
	name: string;
};

export function target<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
>(arg: FunctionArg): Target<A, R>;
export function target<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
>(...args: tg.Args<Target.Arg>): Promise<Target<A, R>>;
export function target<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
>(
	...args: [FunctionArg<A, R>] | tg.Args<Target.Arg>
): MaybePromise<Target<A, R>> {
	if (
		args.length === 1 &&
		typeof args[0] === "object" &&
		"function" in args[0]
	) {
		let arg = args[0];

		// Create the target.
		let args_ = [arg.name];
		let checksum = undefined;
		let executable = {
			kind: arg.module.kind,
			referent: {
				item: tg.Object.withId(arg.module.referent.item),
				path: arg.module.referent.path,
				subpath: arg.module.referent.subpath,
				tag: arg.module.referent.tag,
			},
		};
		const env = currentTarget.state.object!.env;
		let object = {
			args: args_,
			checksum,
			env,
			executable,
			host: "js",
		};
		let state = { object };
		return new Target(state, arg.function);
	} else {
		return Target.new(...(args as tg.Args<Target.Arg>));
	}
}

export interface Target<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
> extends globalThis.Function {
	(...args: { [K in keyof A]: tg.Unresolved<A[K]> }): Promise<R>;
}

// biome-ignore lint/suspicious/noUnsafeDeclarationMerging: This is necessary to make targets callable.
export class Target<
	A extends Array<tg.Value> = Array<tg.Value>,
	R extends tg.Value = tg.Value,
> extends globalThis.Function {
	#state: Target.State;
	#f: Function | undefined;

	constructor(state: Target.State, f?: Function) {
		super();
		this.#state = state;
		this.#f = f;
		let this_ = this as any;
		// biome-ignore lint/correctness/noConstructorReturn: This is necessary to make targets callable.
		return new Proxy(this_, {
			get(_target, prop, _receiver) {
				if (typeof this_[prop] === "function") {
					return this_[prop].bind(this_);
				} else {
					return this_[prop];
				}
			},
			apply: async (target, _, args) => {
				return await (await Target.new(target, { args })).output();
			},
			getPrototypeOf: (_target) => {
				return Object.getPrototypeOf(this_);
			},
		});
	}

	get state(): Target.State {
		return this.#state;
	}

	static withId(id: Target.Id): Target {
		return new Target({ id });
	}

	static async new<
		A extends Array<tg.Value> = Array<tg.Value>,
		R extends tg.Value = tg.Value,
	>(...args: tg.Args<Target.Arg>): Promise<Target<A, R>> {
		let arg = await Target.arg(...args);
		let args_ = arg.args ?? [];
		let checksum = arg.checksum;
		let env = await tg.Args.applyMutations(flatten(arg.env ?? []));
		let executable = arg.executable;
		let host = arg.host;
		if (!host) {
			throw new Error("cannot create a target without a host");
		}
		let object = {
			args: args_,
			checksum,
			env,
			executable,
			host,
		};
		return new Target({ object });
	}

	static get current(): Target {
		return currentTarget;
	}

	static async arg(...args: tg.Args<Target.Arg>): Promise<Target.ArgObject> {
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
						host: (await currentTarget.env()).TANGRAM_HOST as string,
					};
				} else if (arg instanceof Target) {
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

	static expect(value: unknown): Target {
		tg.assert(value instanceof Target);
		return value;
	}

	static assert(value: unknown): asserts value is Target {
		tg.assert(value instanceof Target);
	}

	async id(): Promise<Target.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<Target.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("object_load", this.#state.id!);
			tg.assert(object.kind === "target");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("object_store", {
				kind: "target",
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

	async env(): Promise<{ [key: string]: tg.Value }> {
		return (await this.object()).env;
	}

	async executable(): Promise<tg.Target.Executable | undefined> {
		return (await this.object()).executable;
	}

	async host(): Promise<string> {
		return (await this.object()).host;
	}

	async output(): Promise<R> {
		return (await syscall("build_output", this as Target<[], R>)) as R;
	}

	function(): Function | undefined {
		return this.#f;
	}
}

export namespace Target {
	export type Arg =
		| undefined
		| string
		| tg.Artifact
		| tg.Template
		| Target
		| ArgObject;

	export type ArgObject = {
		args?: Array<tg.Value> | undefined;
		checksum?: tg.Checksum | undefined;
		env?: MaybeNestedArray<MaybeMutationMap> | undefined;
		executable?: tg.Target.ExecutableArg | undefined;
		host?: string | undefined;
	};

	export type Executable = tg.Artifact | tg.Target.Executable.Module;

	export namespace Executable {
		export type Module = {
			kind: tg.Module.Kind;
			referent: tg.Referent<tg.Object>;
		};
	}

	export type ExecutableArg = tg.Artifact | tg.Target.Executable.Module;

	export type Id = string;

	export type Object = {
		args: Array<tg.Value>;
		checksum: tg.Checksum | undefined;
		env: { [key: string]: tg.Value };
		executable: tg.Target.Executable | undefined;
		host: string;
	};

	export type State = tg.Object.State<Target.Id, Target.Object>;
}
