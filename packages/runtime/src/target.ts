import { Args } from "./args.ts";
import { Artifact } from "./artifact.ts";
import { assert as assert_ } from "./assert.ts";
import type { Checksum } from "./checksum.ts";
import { File } from "./file.ts";
import { Module } from "./module.ts";
import type { Object_ } from "./object.ts";
import { type Unresolved, resolve } from "./resolve.ts";
import { symlink } from "./symlink.ts";
import { Template } from "./template.ts";
import {
	type MaybeMutationMap,
	type MaybeNestedArray,
	type MaybePromise,
	flatten,
} from "./util.ts";
import type { Value } from "./value.ts";

let currentTarget: Target;

export let getCurrentTarget = (): Target => {
	return currentTarget;
};

export let setCurrentTarget = (target: Target) => {
	currentTarget = target;
};

type FunctionArg<
	A extends Array<Value> = Array<Value>,
	R extends Value = Value,
> = {
	url: string;
	name: string;
	function: (...args: A) => Unresolved<R>;
};

export function target<
	A extends Array<Value> = Array<Value>,
	R extends Value = Value,
>(arg: FunctionArg): Target<A, R>;
export function target<
	A extends Array<Value> = Array<Value>,
	R extends Value = Value,
>(...args: Args<Target.Arg>): Promise<Target<A, R>>;
export function target<
	A extends Array<Value> = Array<Value>,
	R extends Value = Value,
>(...args: [FunctionArg<A, R>] | Args<Target.Arg>): MaybePromise<Target<A, R>> {
	if (
		args.length === 1 &&
		typeof args[0] === "object" &&
		"function" in args[0]
	) {
		let arg = args[0];

		// Get the module.
		let module_ = Module.fromUrl(arg.url);

		// Create the target.
		let object = {
			args: [arg.name],
			checksum: undefined,
			env: getCurrentTarget().expectObject().env,
			executable: File.withId(module_.object),
			host: "js",
		};
		let state = {
			object: object,
		};
		return new Target(state, arg.function);
	} else {
		return Target.new(...(args as Args<Target.Arg>));
	}
}

export interface Target<
	A extends Array<Value> = Array<Value>,
	R extends Value = Value,
> extends globalThis.Function {
	(...args: { [K in keyof A]: Unresolved<A[K]> }): Promise<R>;
}

// biome-ignore lint/suspicious/noUnsafeDeclarationMerging: This is necessary to make targets callable.
export class Target<
	A extends Array<Value> = Array<Value>,
	R extends Value = Value,
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
		A extends Array<Value> = Array<Value>,
		R extends Value = Value,
	>(...args: Args<Target.Arg>): Promise<Target<A, R>> {
		let arg = await Target.arg(...args);
		let args_ = arg.args ?? [];
		let checksum = arg.checksum;
		let env = await Args.applyMutations(flatten(arg.env ?? []));
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

	static async arg(...args: Args<Target.Arg>): Promise<Target.ArgObject> {
		let resolved = await Promise.all(args.map(resolve));
		let flattened = flatten(resolved);
		let objects = await Promise.all(
			flattened.map(async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (
					typeof arg === "string" ||
					Artifact.is(arg) ||
					arg instanceof Template
				) {
					return {
						args: ["-c", arg],
						executable: await symlink("/bin/sh"),
						host: (await getCurrentTarget().env()).TANGRAM_HOST as string,
					};
				} else if (arg instanceof Target) {
					return await arg.object();
				} else {
					return arg;
				}
			}),
		);
		let mutations = await Args.createMutations(objects, {
			args: "append",
			env: "append",
		});
		let arg = await Args.applyMutations(mutations);
		return arg;
	}

	static expect(value: unknown): Target {
		assert_(value instanceof Target);
		return value;
	}

	static assert(value: unknown): asserts value is Target {
		assert_(value instanceof Target);
	}

	async id(): Promise<Target.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<Target.Object_> {
		await this.load();
		return this.#state.object!;
	}

	expectId(): Target.Id {
		if (!this.#state.id) {
			throw new Error("expected the object to be stored");
		}
		return this.#state.id;
	}

	expectObject(): Target.Object_ {
		if (!this.#state.object) {
			throw new Error("expected the object to be loaded");
		}
		return this.#state.object;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("load", this.#state.id!);
			assert_(object.kind === "target");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("store", {
				kind: "target",
				value: this.#state.object!,
			});
		}
	}

	async args(): Promise<Array<Value>> {
		return (await this.object()).args;
	}

	async checksum(): Promise<Checksum | undefined> {
		return (await this.object()).checksum;
	}

	async env(): Promise<{ [key: string]: Value }> {
		return (await this.object()).env;
	}

	async executable(): Promise<File | undefined> {
		return (await this.object()).executable;
	}

	async host(): Promise<string> {
		return (await this.object()).host;
	}

	async output(): Promise<R> {
		return (await syscall("output", this as Target<[], R>)) as R;
	}

	function(): Function | undefined {
		return this.#f;
	}
}

export namespace Target {
	export type Arg =
		| undefined
		| string
		| Artifact
		| Template
		| Target
		| ArgObject;

	export type ArgObject = {
		args?: Array<Value> | undefined;
		checksum?: Checksum | undefined;
		env?: MaybeNestedArray<MaybeMutationMap> | undefined;
		executable?: File | undefined;
		host?: string | undefined;
	};

	export type Id = string;

	export type Object_ = {
		args: Array<Value>;
		checksum: Checksum | undefined;
		env: { [key: string]: Value };
		executable: File | undefined;
		host: string;
	};

	export type State = Object_.State<Target.Id, Target.Object_>;
}
