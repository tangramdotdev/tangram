import type { Cancel as ProcessCancel } from "./client/process/cancel.ts";
import type { Get as ProcessGet } from "./client/process/get.ts";
import type { Put as ProcessPut } from "./client/process/put.ts";
import { Spawn as ProcessSpawn } from "./client/process/spawn.ts";
import * as tg from "./index.ts";
import * as build from "./process/build.ts";
import * as exec from "./process/exec.ts";
import * as run from "./process/run.ts";
import * as spawn from "./process/spawn.ts";
import * as stdio from "./process/stdio.ts";

export let process: {
	args: Array<tg.Value>;
	cwd: string;
	env: { [key: string]: tg.Value };
	export: string | null;
	module: tg.Module;
} = {} as any;

export let setProcess = (newProcess: typeof process) => {
	Object.defineProperties(
		process,
		Object.getOwnPropertyDescriptors(newProcess),
	);
};

export class Process<O extends tg.Value = tg.Value> {
	#id: number | tg.Process.Id;
	#lease: string | null;
	#location: tg.Location.Arg | null;
	#owned: boolean;
	#options: tg.Referent.Options;
	#promise: Promise<tg.Process.Wait> | null;
	#state: tg.Process.State | null;
	#stderr: tg.Process.Stdio.Reader;
	#stdin: tg.Process.Stdio.Writer;
	#stdioPromise: Promise<void> | null;
	#stopper: tg.Host.Stopper | null;
	#stdout: tg.Process.Stdio.Reader;
	#tokens: tg.Authorization.Tokens;
	#wait: tg.Process.Wait | null;

	static build<
		A extends tg.UnresolvedArgs<Array<tg.Value>>,
		O extends tg.ReturnValue,
	>(
		function_: (...args: A) => O,
	): tg.Process.Builder<"run", [], tg.ResolvedReturnValue<O>>;
	static build<
		A extends tg.UnresolvedArgs<Array<tg.Value>>,
		O extends tg.ReturnValue,
	>(
		function_: (...args: A) => O,
		...args: tg.UnresolvedArgs<tg.ResolvedArgs<A>>
	): tg.Process.Builder<"run", [], tg.ResolvedReturnValue<O>>;
	static build(
		strings: TemplateStringsArray,
		...placeholders: tg.Args<tg.Template.Arg>
	): tg.Process.Builder<"run", Array<tg.Value>, tg.Value>;
	static build(
		...args: tg.Args<tg.Process.Arg>
	): tg.Process.Builder<"run", Array<tg.Value>, tg.Value>;
	static build(...args: any): any {
		return build.builder(...args);
	}

	static exec<
		A extends tg.UnresolvedArgs<Array<tg.Value>>,
		O extends tg.ReturnValue,
	>(function_: (...args: A) => O): tg.Process.Builder<"exec", [], never>;
	static exec<
		A extends tg.UnresolvedArgs<Array<tg.Value>>,
		O extends tg.ReturnValue,
	>(
		function_: (...args: A) => O,
		...args: tg.UnresolvedArgs<tg.ResolvedArgs<A>>
	): tg.Process.Builder<"exec", [], never>;
	static exec(
		strings: TemplateStringsArray,
		...placeholders: tg.Args<tg.Template.Arg>
	): tg.Process.Builder<"exec", Array<tg.Value>, never>;
	static exec(
		...args: tg.Args<tg.Process.Arg>
	): tg.Process.Builder<"exec", Array<tg.Value>, never>;
	static exec(...args: any): any {
		return exec.builder(...args);
	}

	static run<
		A extends tg.UnresolvedArgs<Array<tg.Value>>,
		O extends tg.ReturnValue,
	>(
		function_: (...args: A) => O,
	): tg.Process.Builder<"run", [], tg.ResolvedReturnValue<O>>;
	static run<
		A extends tg.UnresolvedArgs<Array<tg.Value>>,
		O extends tg.ReturnValue,
	>(
		function_: (...args: A) => O,
		...args: tg.UnresolvedArgs<tg.ResolvedArgs<A>>
	): tg.Process.Builder<"run", [], tg.ResolvedReturnValue<O>>;
	static run(
		strings: TemplateStringsArray,
		...placeholders: tg.Args<tg.Template.Arg>
	): tg.Process.Builder<"run", Array<tg.Value>, tg.Value>;
	static run(
		...args: tg.Args<tg.Process.Arg>
	): tg.Process.Builder<"run", Array<tg.Value>, tg.Value>;
	static run(...args: any): any {
		return run.builder(...args);
	}

	static spawn<
		A extends tg.UnresolvedArgs<Array<tg.Value>>,
		O extends tg.ReturnValue,
	>(
		function_: (...args: A) => O,
	): tg.Process.Builder<"spawn", [], tg.ResolvedReturnValue<O>>;
	static spawn<
		A extends tg.UnresolvedArgs<Array<tg.Value>>,
		O extends tg.ReturnValue,
	>(
		function_: (...args: A) => O,
		...args: tg.UnresolvedArgs<tg.ResolvedArgs<A>>
	): tg.Process.Builder<"spawn", [], tg.ResolvedReturnValue<O>>;
	static spawn(
		strings: TemplateStringsArray,
		...placeholders: tg.Args<tg.Template.Arg>
	): tg.Process.Builder<"spawn", Array<tg.Value>, tg.Value>;
	static spawn(
		...args: tg.Args<tg.Process.Arg>
	): tg.Process.Builder<"spawn", Array<tg.Value>, tg.Value>;
	static spawn(...args: any): any {
		return spawn.builder(...args);
	}

	static async arg(
		...args: tg.Args<tg.Process.Arg>
	): Promise<tg.Process.ResolvedArgObject> {
		return await tg.Process.argResolved(
			...(await Promise.all(args.map(tg.resolve))),
		);
	}

	static async argResolved(
		...args: Array<tg.ValueOrMaybeMutationMap<tg.Process.Arg>>
	): Promise<tg.Process.ResolvedArgObject> {
		return await tg.Args.applyResolved<
			tg.Process.Arg,
			tg.Process.MappedArg,
			tg.Process.ResolvedArgObject
		>({
			args,
			map: async (arg): Promise<tg.Process.MappedArg> => {
				let output: tg.ValueOrMaybeMutationMap<tg.Process.Arg>;
				if (arg === undefined) {
					output = {};
				} else if (
					typeof arg === "string" ||
					tg.Artifact.is(arg) ||
					arg instanceof tg.Template
				) {
					let executable =
						typeof tg.process.env.SHELL === "string"
							? tg.process.env.SHELL
							: "sh";
					output = {
						args: ["-c", arg],
						executable,
					};
				} else if (arg instanceof tg.Command) {
					let object = await arg.object();
					output = {
						args: object.args,
						env: object.env,
						executable: object.executable,
						host: object.host,
					};
					if (object.cwd !== null) {
						output.cwd = object.cwd;
					}
					if (object.stdin !== null) {
						output.stdin = object.stdin;
					}
					if (object.user !== null) {
						output.user = object.user;
					}
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
				} as tg.Process.MappedArg;
			},
			reduce: {
				args: (a, b) => [...(a ?? []), ...(b ?? [])],
				env: tg.Command.Arg.Env.reduce,
				mounts: "append",
				ports: "append",
			},
		});
	}

	static async spawnArg(...args: tg.Args<tg.Process.Arg>): Promise<{
		arg: tg.Process.Spawn.Arg;
		options: tg.Referent.Options;
	}> {
		return await spawn.spawnArg(...args);
	}

	static async spawnArgFromResolved(arg: tg.Process.ArgObject): Promise<{
		arg: tg.Process.Spawn.Arg;
		options: tg.Referent.Options;
	}> {
		return await spawn.spawnArgFromResolved(arg);
	}

	static async execUnsandboxed(arg: tg.Process.Spawn.Arg): Promise<never> {
		return await exec.execUnsandboxed(arg);
	}

	static async spawnUnsandboxed<O extends tg.Value = tg.Value>(
		arg: tg.Process.Spawn.Arg,
		options?: tg.Referent.Options | null,
	): Promise<tg.Process<O>> {
		return await spawn.spawnUnsandboxed<O>(arg, options);
	}

	static async waitUnsandboxed(
		pid: number,
		stdio: {
			stderr: tg.Process.Stdio.Reader;
			stdin: tg.Process.Stdio.Writer;
			stdout: tg.Process.Stdio.Reader;
		},
		stopper: tg.Host.Stopper,
		tempPath: string,
		outputPath: string,
	): Promise<tg.Process.Wait> {
		return await spawn.waitUnsandboxed(
			pid,
			stdio,
			stopper,
			tempPath,
			outputPath,
		);
	}

	static async prepareUnsandboxedCommand(
		arg: tg.Process.Spawn.Arg,
		outputPath?: string | null,
	): Promise<tg.Process.PreparedUnsandboxedCommandOutput> {
		return await spawn.prepareUnsandboxedCommand(arg, outputPath);
	}

	static async spawnSandboxed<O extends tg.Value = tg.Value>(
		arg: tg.Process.Spawn.Arg,
		options?: tg.Referent.Options | null,
	): Promise<tg.Process<O>> {
		return await spawn.spawnSandboxed<O>(arg, options);
	}

	constructor(arg: tg.Process.ConstructorArg) {
		this.#id = arg.id;
		this.#lease = arg.lease ?? null;
		this.#location = arg.location ?? null;
		this.#options = arg.options ?? {};
		this.#state = arg.state ?? null;
		this.#stdioPromise = arg.stdioPromise ?? null;
		this.#promise =
			arg.promise === undefined || arg.promise === null
				? null
				: arg.promise.finally(() => this.detach());
		this.#stdin = arg.stdin;
		this.#stdout = arg.stdout;
		this.#stderr = arg.stderr;
		this.#stopper = arg.stopper ?? null;
		this.#tokens = arg.tokens ?? {};
		this.#wait = arg.wait ?? null;
		this.#owned =
			this.#wait === null &&
			(typeof this.#id === "number"
				? this.#stopper !== null
				: this.#lease !== null);
		this.#stdin.setProcess(this);
		this.#stdout.setProcess(this);
		this.#stderr.setProcess(this);
	}

	get state(): tg.Process.State | null {
		return this.#state;
	}

	/** Expect that a value is a `tg.Process`. */
	static expect(value: unknown): tg.Process {
		tg.assert(value instanceof Process);
		return value;
	}

	/** Assert that a value is a `tg.Process`. */
	static assert(value: unknown): asserts value is tg.Process {
		tg.assert(value instanceof Process);
	}

	/** Load the process's state. */
	async load(): Promise<void> {
		if (typeof this.#id === "number") {
			throw new Error("loading unsandboxed process state is not supported");
		}
		let arg: tg.Process.Get.Arg = {};
		if (this.#location !== null) {
			arg.location = this.#location;
		}
		arg.tokens = this.#tokens;
		let output = await tg.client.getProcess(this.#id, arg);
		if (output.tokens !== undefined && output.tokens !== null) {
			tg.Authorization.Tokens.inherit(this.#tokens, output.tokens);
		}
		this.#location =
			output.location === undefined || output.location === null
				? null
				: tg.Location.Arg.fromLocation(output.location);
		this.#state = tg.Process.State.fromData(output.data);
		tg.Process.State.inheritTokens(this.#state, this.#tokens);
	}

	/** Reload the process's state. */
	async reload(): Promise<void> {
		await this.load();
	}

	async #getSandbox(): Promise<tg.Sandbox.Get.Output | null> {
		if (typeof this.#id === "number") {
			return null;
		}
		await this.load();
		return await tg.client.getSandbox(this.#state!.sandbox);
	}

	/** Get this process's ID. */
	get id(): number | tg.Process.Id {
		return this.#id;
	}

	/** Get this process's location arg. */
	get location(): tg.Location.Arg | null {
		return this.#location ?? null;
	}

	get tokens(): tg.Authorization.Tokens {
		return { ...this.#tokens };
	}

	set tokens(tokens: tg.Authorization.Tokens) {
		this.#tokens = tokens;
	}

	inheritTokens(tokens: tg.Authorization.Tokens): void {
		tg.Authorization.Tokens.inherit(this.#tokens, tokens);
	}

	/** Get this process's command. */
	get command(): Promise<tg.Command> {
		return (async () => {
			await this.load();
			let command = this.#state!.command;

			tg.Object.inheritTokens(command, this.#tokens);

			return command;
		})();
	}

	/** Get this process's command's args. */
	get args(): Promise<Array<tg.Command.Value>> {
		return (async () => {
			return await (
				await this.command
			).args;
		})();
	}

	/** Get this process's command's cwd. */
	get cwd(): Promise<string | null> {
		return (async () => {
			return await (
				await this.command
			).cwd;
		})();
	}

	/** Get this process's command's environment. */
	async env(): Promise<{ [key: string]: tg.Command.Value }>;
	async env(name: string): Promise<tg.Command.Value | undefined>;
	async env(
		name?: string,
	): Promise<
		{ [name: string]: tg.Command.Value } | tg.Command.Value | undefined
	> {
		let env = await (await this.command).env;
		if (name === undefined) {
			return { ...env };
		} else {
			return env[name];
		}
	}

	/** Get this process's command's executable. */
	get executable(): Promise<tg.Command.Executable> {
		return (async () => {
			return await (
				await this.command
			).executable;
		})();
	}

	get mounts(): Promise<Array<tg.Sandbox.Mount>> {
		return (async () => {
			let sandbox = await this.#getSandbox();
			return (sandbox?.mounts ?? []).map(tg.Sandbox.Mount.fromDataString);
		})();
	}

	get network(): Promise<boolean> {
		return (async () => {
			let sandbox = await this.#getSandbox();
			return sandbox?.network !== undefined && sandbox.network !== null;
		})();
	}

	get ports(): Promise<Array<tg.Sandbox.Port>> {
		return (async () => {
			let sandbox = await this.#getSandbox();
			let network = sandbox?.network;
			if (network?.kind !== "bridge") {
				return [];
			}
			return (network.ports ?? []).map(tg.Sandbox.Port.fromDataString);
		})();
	}

	/** Get this process's sandbox. */
	get sandbox(): Promise<string | null> {
		return (async () => {
			if (typeof this.#id === "number") {
				return null;
			}
			await this.load();
			return this.#state!.sandbox;
		})();
	}

	/** Get this process's command's user. */
	get user(): Promise<string | null> {
		return (async () => {
			return await (
				await this.command
			).user;
		})();
	}

	/** Get this process's stdin writer. */
	get stdin(): tg.Process.Stdio.Writer {
		return this.#stdin;
	}

	/** Get this process's stdout reader. */
	get stdout(): tg.Process.Stdio.Reader {
		return this.#stdout;
	}

	/** Get this process's stderr reader. */
	get stderr(): tg.Process.Stdio.Reader {
		return this.#stderr;
	}

	/** Get this process's lease. */
	get lease(): string | null {
		return this.#lease;
	}

	/** Cancel this process. */
	async cancel(): Promise<void> {
		if (typeof this.#id === "number") {
			if (this.#stopper === null) {
				await tg.host.signal(this.#id, tg.Process.Signal.TERM);
			} else {
				await tg.host.stopperStop(this.#stopper);
				if (this.#promise !== null) {
					await this.#promise;
				}
			}
		} else {
			if (this.#lease === null) {
				throw new Error("missing lease");
			}
			await tg.client.cancelProcess(this.#id, {
				lease: this.#lease,
				...(this.#location === null ? {} : { location: this.#location }),
			});
		}
		this.detach();
	}

	/** Detach this process from this handle's lifetime. */
	detach(): void {
		if (!this.#owned) {
			return;
		}
		this.#owned = false;
	}

	async [Symbol.asyncDispose](): Promise<void> {
		if (this.#owned) {
			await this.cancel();
		}
	}

	/** Send a signal to this process. */
	async signal(signal: tg.Process.Signal): Promise<void> {
		if (typeof this.#id === "number") {
			await tg.host.signal(this.#id, signal);
			return;
		}
		let location = this.#location;
		if (location === null) {
			await this.load();
			location = this.#location;
		}
		let arg: tg.Signal.Arg = { signal };
		if (location !== null) {
			arg.location = location;
		}
		arg.tokens = this.#tokens;
		await tg.client.signalProcess(this.#id, arg);
	}

	/** Wait for this process to exit. */
	async wait(): Promise<tg.Process.Wait> {
		if (this.#stdioPromise !== null) {
			await this.#stdioPromise;
		}
		if (this.#wait !== null) {
			tg.Process.Wait.inheritTokens(this.#wait, this.#tokens);
			return this.#wait;
		}
		if (typeof this.#id === "number") {
			tg.assert(this.#promise !== null);
			let wait = await this.#promise;
			tg.Process.Wait.inheritTokens(wait, this.#tokens);
			this.#wait = wait;
			this.detach();
			return wait;
		}
		let arg: tg.Process.Wait.Arg = {};
		if (this.#lease !== null) {
			arg.lease = this.#lease;
		}
		if (this.#location !== null) {
			arg.location = this.#location;
		}
		arg.tokens = this.#tokens;
		let promise = await tg.client.waitProcessPromise(this.#id, arg);
		let wait = await promise();
		if (wait === null) {
			throw new Error("failed to find the process");
		}
		tg.Process.Wait.inheritTokens(wait, this.#tokens);
		this.#wait = wait;
		this.detach();
		return wait;
	}

	/** Wait for this process to exit and return the output. */
	async output(): Promise<O> {
		let wait = await this.wait();

		if (wait.error !== null) {
			let error = wait.error;
			const options = {
				...this.#options,
				tokens: error.state.tokens,
			};
			const source = {
				node: error,
				options,
			};
			const values: { [key: string]: string } = {
				id: String(this.id),
			};
			if (this.#options.name !== undefined && this.#options.name !== null) {
				values.name = this.#options.name;
			}
			throw tg.error.sync("the child process failed", {
				source,
				values,
			});
		}
		if (wait.exit >= 1 && wait.exit < 128) {
			const error = tg.error.sync(`the process exited with code ${wait.exit}`);
			const source = {
				node: error,
				options: this.#options,
			};
			const values: { [key: string]: string } = {
				id: String(this.id),
			};
			if (this.#options.name !== undefined && this.#options.name !== null) {
				values.name = this.#options.name;
			}
			throw tg.error.sync("the child process failed", {
				source,
				values,
			});
		}
		if (wait.exit >= 128) {
			const error = tg.error.sync(`the process exited with code ${wait.exit}`);
			const source = {
				node: error,
				options: this.#options,
			};
			const values: { [key: string]: string } = {
				id: String(this.id),
			};
			if (this.#options.name !== undefined && this.#options.name !== null) {
				values.name = this.#options.name;
			}
			throw tg.error.sync(
				`the child process exited with signal ${wait.exit - 128}`,
				{
					source,
					values,
				},
			);
		}

		let output = wait.output;

		if (output !== undefined) {
			tg.Value.inheritTokens(output, this.#tokens);
		}

		return output as O;
	}

	/** Set this process's tty size. */
	async setTtySize(size: tg.Process.Tty.Size): Promise<void> {
		if (typeof this.#id === "number") {
			throw new Error(
				"tty resizing is not supported for unsandboxed processes",
			);
		}
		let location = this.#location;
		if (location === null) {
			await this.load();
			location = this.#location;
		}
		let arg: tg.Process.Tty.Put.Arg = { size };
		if (location !== null) {
			arg.location = location;
		}
		arg.tokens = this.#tokens;
		await tg.client.setProcessTtySize(this.#id, arg);
	}
}

export namespace Process {
	export type Id = string;

	export namespace Cancel {
		export type Arg = ProcessCancel.Arg;
	}

	export namespace Get {
		export type Arg = ProcessGet.Arg;

		export type Output = ProcessGet.Output;
	}

	export namespace Put {
		export type Arg = ProcessPut.Arg;

		export type Output = ProcessPut.Output;
	}

	export namespace Spawn {
		export import Arg = ProcessSpawn.Arg;
		export type CommandArg = ProcessSpawn.CommandArg;

		export import Output = ProcessSpawn.Output;
	}

	export namespace Tty {
		export namespace Put {
			export type Arg = {
				location?: tg.Location.Arg | null;
				size: tg.Process.Tty.Size;
				tokens?: tg.Authorization.Tokens | null;
			};
		}
	}

	export interface Builder<
		M extends tg.Process.Builder.Mode,
		A extends Array<tg.Value> = Array<tg.Value>,
		O extends tg.Value = tg.Value,
		E = tg.Command.Arg.Env,
	> {
		(...args: tg.UnresolvedArgs<A>): tg.Process.Builder<M, [], O, E>;
	}

	export class Builder<
		M extends tg.Process.Builder.Mode,
		A extends Array<tg.Value> = Array<tg.Value>,
		O extends tg.Value = tg.Value,
		E = tg.Command.Arg.Env,
	> extends Function {
		#args: tg.Args<tg.Process.Arg>;
		#envMapper: tg.Process.Builder.EnvMapper<E>;
		#js: Promise<boolean>;
		#mode: M;
		#validate?: (arg: tg.Process.ArgObject) => void;

		constructor(mode: M, ...args: tg.Args<tg.Process.Arg>) {
			super();
			this.#envMapper = ((env: tg.Command.Arg.Env) =>
				env) as tg.Process.Builder.EnvMapper<E>;
			this.#js = isJsProcessBuilderArg(args);
			this.#args = args.map((arg) => this.builderArg(arg));
			this.#mode = mode;
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

		arg(...args: Array<tg.Unresolved<tg.Command.Arg.Value>>): this {
			return this.args(args);
		}

		args(
			...args: Array<tg.Unresolved<Array<tg.Command.Arg.Value> | null>>
		): this {
			this.#args.push(...args.map((args) => this.argsArg(args)));
			return this;
		}

		checksum(
			checksum: tg.Unresolved<tg.MaybeMutation<tg.Checksum> | null>,
		): this {
			this.#args.push({ checksum });
			return this;
		}

		cwd(cwd: tg.Unresolved<tg.MaybeMutation<string> | null>): this {
			this.#args.push({ cwd });
			return this;
		}

		debug(
			debug: tg.Unresolved<tg.MaybeMutation<
				boolean | tg.Process.Debug
			> | null> = true,
		): this {
			this.#args.push({ debug });
			return this;
		}

		cpu(cpu: tg.Unresolved<tg.MaybeMutation<number> | null>): this {
			this.#args.push({ cpu });
			return this;
		}

		env(...envs: Array<tg.Unresolved<E | null>>): this {
			this.#args.push(...envs.map((env) => this.envArg(env)));
			return this;
		}

		envMapper<E_>(
			envMapper: tg.Process.Builder.EnvMapper<E_>,
		): tg.Process.Builder<M, A, O, E_> {
			let builder = this as unknown as tg.Process.Builder<M, A, O, E_>;
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

		location(
			location: tg.Unresolved<tg.MaybeMutation<tg.Location.Arg> | null>,
		): this {
			this.#args.push({ location });
			return this;
		}

		memory(memory: tg.Unresolved<tg.MaybeMutation<number> | null>): this {
			this.#args.push({ memory });
			return this;
		}

		mount(...mounts: Array<tg.Unresolved<tg.Sandbox.Mount>>): this {
			this.#args.push({ mounts });
			return this;
		}

		mounts(
			...mounts: Array<
				tg.Unresolved<tg.MaybeMutation<Array<tg.Sandbox.Mount>> | null>
			>
		): this {
			this.#args.push(...mounts.map((mounts) => ({ mounts })));
			return this;
		}

		named(name: tg.Unresolved<tg.MaybeMutation<string> | null>): this {
			this.#args.push({ name });
			return this;
		}

		network(): this;
		network(
			network: tg.Unresolved<tg.MaybeMutation<
				boolean | tg.Sandbox.Network
			> | null>,
		): this;
		network(
			network?: tg.Unresolved<tg.MaybeMutation<
				boolean | tg.Sandbox.Network
			> | null>,
		): this {
			this.#args.push({ network: network === undefined ? true : network });
			return this;
		}

		port(...ports: Array<tg.Unresolved<tg.Sandbox.Port>>): this {
			this.#args.push({ ports });
			return this;
		}

		ports(
			...ports: Array<
				tg.Unresolved<tg.MaybeMutation<Array<tg.Sandbox.Port>> | null>
			>
		): this {
			this.#args.push(...ports.map((ports) => ({ ports })));
			return this;
		}

		sandbox(): this;
		sandbox(
			sandbox: tg.Unresolved<tg.MaybeMutation<
				boolean | tg.Sandbox.Arg | tg.Sandbox.Id
			> | null>,
		): this;
		sandbox(
			sandbox?: tg.Unresolved<tg.MaybeMutation<
				boolean | tg.Sandbox.Arg | tg.Sandbox.Id
			> | null>,
		): this {
			this.#args.push({ sandbox: sandbox === undefined ? true : sandbox });
			return this;
		}

		stderr(
			stderr: tg.Unresolved<tg.MaybeMutation<tg.Process.Stdio> | null>,
		): this {
			this.#args.push({ stderr });
			return this;
		}

		stdin(
			stdin: tg.Unresolved<tg.MaybeMutation<
				tg.Blob.Arg | tg.Process.Stdio
			> | null>,
		): this {
			this.#args.push({ stdin });
			return this;
		}

		stdio(
			stdio: tg.Unresolved<tg.MaybeMutation<tg.Process.Stdio> | null>,
		): this {
			this.#args.push({ stdin: stdio, stdout: stdio, stderr: stdio });
			return this;
		}

		stdout(
			stdout: tg.Unresolved<tg.MaybeMutation<tg.Process.Stdio> | null>,
		): this {
			this.#args.push({ stdout });
			return this;
		}

		tty(
			tty: tg.Unresolved<tg.MaybeMutation<boolean | tg.Process.Tty> | null>,
		): this {
			this.#args.push({ tty });
			return this;
		}

		validate(validate: (arg: tg.Process.ArgObject) => void): this {
			this.#validate = validate;
			return this;
		}

		exec(): tg.Process.Builder<"exec", A, never, E> {
			let output = new tg.Process.Builder<"exec", A, never, E>(
				"exec",
				...this.#args,
			);
			output.envMapper<E>(this.#envMapper);
			if (this.#validate !== undefined) {
				output.validate(this.#validate);
			}
			return output;
		}

		run(): tg.Process.Builder<"run", A, O, E> {
			let output = new tg.Process.Builder<"run", A, O, E>("run", ...this.#args);
			output.envMapper<E>(this.#envMapper);
			if (this.#validate !== undefined) {
				output.validate(this.#validate);
			}
			return output;
		}

		spawn(): tg.Process.Builder<"spawn", A, O, E> {
			let output = new tg.Process.Builder<"spawn", A, O, E>(
				"spawn",
				...this.#args,
			);
			output.envMapper<E>(this.#envMapper);
			if (this.#validate !== undefined) {
				output.validate(this.#validate);
			}
			return output;
		}

		then<TResult1 = tg.Process.Builder.Output<M, O>, TResult2 = never>(
			onfulfilled?:
				| ((
						value: tg.Process.Builder.Output<M, O>,
				  ) => TResult1 | PromiseLike<TResult1>)
				| undefined
				| null,
			onrejected?:
				| ((reason: any) => TResult2 | PromiseLike<TResult2>)
				| undefined
				| null,
		): PromiseLike<TResult1 | TResult2> {
			return this.#thenInner().then(onfulfilled, onrejected);
		}

		#thenInner(): Promise<tg.Process.Builder.Output<M, O>>;
		async #thenInner(): Promise<O | tg.Process<O>> {
			let arg = await tg.Process.arg(...this.#args);
			this.#validate?.(arg);
			let output = await spawn.spawnArg(...this.#args);
			if (this.#mode === "exec") {
				return await tg.Process.execUnsandboxed(output.arg);
			}
			let process =
				output.arg.sandbox === undefined
					? await tg.Process.spawnUnsandboxed<O>(output.arg, output.options)
					: await tg.Process.spawnSandboxed<O>(output.arg, output.options);
			if (this.#mode === "spawn") {
				return process;
			}
			return await process.output();
		}

		private async builderArg(
			arg: tg.Unresolved<tg.ValueOrMaybeMutationMap<tg.Process.Arg>>,
		): Promise<tg.ValueOrMaybeMutationMap<tg.Process.Arg>> {
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
		): Promise<tg.Process.ArgObject> {
			let [js, args_] = await Promise.all([this.#js, tg.resolve(args)]);
			if (!js || args_ === null) {
				return { args: args_ };
			}
			let output = encodeJsArgs(args_);

			return { args: output };
		}

		private envArg(
			env: tg.Unresolved<E | null>,
		): tg.Unresolved<tg.Process.ArgObject> {
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
		export type EnvMapper<E> = tg.Command.Builder.EnvMapper<E>;

		export type Mode = "exec" | "run" | "spawn";

		export type Output<
			M extends tg.Process.Builder.Mode,
			O extends tg.Value,
		> = M extends "exec"
			? never
			: M extends "run"
				? O
				: M extends "spawn"
					? tg.Process<O>
					: never;
	}

	export type ConstructorArg = {
		id: number | tg.Process.Id;
		lease?: string | null;
		location?: tg.Location.Arg | null;
		options?: tg.Referent.Options;
		promise?: Promise<tg.Process.Wait> | null;
		state?: State | null;
		stderr: tg.Process.Stdio.Reader;
		stdin: tg.Process.Stdio.Writer;
		stdioPromise?: Promise<void> | null;
		stopper?: tg.Host.Stopper | null;
		stdout: tg.Process.Stdio.Reader;
		tokens?: tg.Authorization.Tokens | null;
		wait?: tg.Process.Wait | null;
	};

	export type PreparedUnsandboxedCommandOutput = {
		args: Array<string>;
		cwd: string | null;
		env: { [key: string]: string };
		executable: string;
		outputPath: string;
		tempPath: string;
	};

	export type Arg = string | tg.Artifact | tg.Template | tg.Command | ArgObject;

	export type MappedArg = Omit<
		tg.ValueOrMaybeMutationMap<tg.Process.ArgObject>,
		"args" | "env"
	> & {
		args?: Array<tg.Command.Value> | null;
		env?: tg.Command.Arg.Env | null;
	};

	export type ResolvedArgObject = Omit<tg.Process.ArgObject, "args" | "env"> & {
		args?: Array<tg.Command.Value> | null;
		env?: { [key: string]: tg.Command.Value } | null;
	};

	export type ArgObject = {
		/** The command's arguments. */
		args?: Array<tg.Command.Arg.Value> | null;

		/** The cache location arg. */
		cache_location?: tg.Location.Arg | null;

		/** If a checksum of the process's output is provided, then the process can be cached even if it is not sandboxed. */
		checksum?: tg.Checksum | null;

		/** The base command. */
		command?: tg.MaybeReferent<tg.Command> | null;

		/** The sandbox's CPU allocation. */
		cpu?: number | null;

		/** The command's working directory. */
		cwd?: string | null;

		/** Configure debugging. */
		debug?: boolean | tg.Process.Debug | null;

		/** The command's environment. */
		env?: tg.Command.Arg.Env | null;

		/** The command's executable. */
		executable?: tg.Command.Arg.Executable | null;

		/** The command's host. */
		host?: string | null;

		/** The process location arg. */
		location?: tg.Location.Arg | null;

		/** The sandbox's memory allocation. */
		memory?: number | null;

		/** Configure mounts. */
		mounts?: Array<tg.Sandbox.Mount> | null;

		/** The process's name. */
		name?: string | null;

		/** Configure network. */
		network?: boolean | tg.Sandbox.Network | null;

		/** The sandbox owner. */
		owner?: string | null;

		/** Configure port forwarding. */
		ports?: Array<tg.Sandbox.Port> | null;

		/** Configure or select the sandbox for this process. */
		sandbox?: boolean | tg.Sandbox.Arg | tg.Sandbox.Id | null;

		/** Configure stderr. */
		stderr?: tg.Process.Stdio | null;

		/** Configure stdin, or set it to a blob. */
		stdin?: tg.Blob.Arg | tg.Process.Stdio | null;

		/** Configure stdout. */
		stdout?: tg.Process.Stdio | null;

		/** Configure whether the process should allocate a tty. */
		tty?: boolean | tg.Process.Tty | null;

		/** The command's user. */
		user?: string | null;
	};

	export type State = {
		actualChecksum: tg.Checksum | null;
		cacheable: boolean;
		children: Array<tg.Process.Child> | null;
		command: tg.Command;
		createdAt: number;
		debug: tg.Process.Debug | null;
		error: tg.Error | null;
		exit: number | null;
		expectedChecksum: tg.Checksum | null;
		finishedAt: number | null;
		host: string;
		log: tg.Blob | null;
		output?: tg.Value;
		retry: boolean;
		sandbox: string;
		startedAt: number | null;
		status: tg.Process.Status;
		stderr: tg.Process.Stdio;
		stdin: tg.Process.Stdio;
		stdout: tg.Process.Stdio;
		tty: tg.Process.Tty | null;
	};

	export type Child = {
		cached: boolean;
		options: tg.Referent.Options;
		process: tg.Process;
	};

	export type Debug = {
		addr?: string | null;
		mode?: tg.Process.Debug.Mode | null;
	};

	export namespace Debug {
		export type Mode = "normal" | "break" | "wait";
	}

	export namespace Child {
		export let toData = (value: tg.Process.Child): tg.Process.Data.Child => {
			let process = value.process.id;
			if (typeof process !== "string") {
				throw new Error("expected a sandboxed process id");
			}
			let tokens = value.process.tokens;
			let options = {
				...value.options,
				tokens,
			};
			let referent = { node: process, options };
			return {
				cached: value.cached,
				process: tg.Referent.toDataString(referent, (id) => id),
			};
		};

		export let fromData = (data: tg.Process.Data.Child): tg.Process.Child => {
			let referent = tg.Referent.fromDataString(
				data.process,
				(id) => id as tg.Process.Id,
			);
			let options = { ...referent.options };
			delete options.tokens;
			return {
				cached: data.cached ?? false,
				options,
				process: new tg.Process({
					id: referent.node,
					stderr: new tg.Process.Stdio.Reader({
						stream: "stderr",
					}),
					stdin: new tg.Process.Stdio.Writer({
						stream: "stdin",
					}),
					stdout: new tg.Process.Stdio.Reader({
						stream: "stdout",
					}),
					...(referent.options?.tokens !== undefined &&
					referent.options.tokens !== null
						? { tokens: referent.options.tokens }
						: {}),
				}),
			};
		};
	}

	export namespace State {
		export let inheritTokens = (
			state: State,
			tokens: tg.Authorization.Tokens,
		): void => {
			tg.Object.inheritTokens(state.command, tokens);
			for (let child of state.children ?? []) {
				child.process.inheritTokens(tokens);
			}
			if (state.error !== null) {
				tg.Object.inheritTokens(state.error, tokens);
			}
			if (state.log !== null) {
				tg.Object.inheritTokens(state.log, tokens);
			}
			if (state.output !== undefined) {
				tg.Value.inheritTokens(state.output, tokens);
			}
		};

		export let toData = (value: State): Data => {
			let output: Data = {
				command: value.command.id,
				created_at: value.createdAt,
				host: value.host,
				sandbox: value.sandbox,
				status: value.status,
			};
			if (value.actualChecksum !== null) {
				output.actual_checksum = value.actualChecksum;
			}
			if (value.cacheable) {
				output.cacheable = value.cacheable;
			}
			if (value.children !== null) {
				output.children = value.children.map(tg.Process.Child.toData);
			}
			if (value.debug !== null) {
				output.debug = value.debug;
			}
			if (value.error !== null) {
				output.error = tg.Error.toDataOrId(value.error);
			}
			if (value.exit !== null) {
				output.exit = value.exit;
			}
			if (value.expectedChecksum !== null) {
				output.expected_checksum = value.expectedChecksum;
			}
			if (value.finishedAt !== null) {
				output.finished_at = value.finishedAt;
			}
			if (value.log !== null) {
				let tokens = value.log.state.tokens;
				let referent = tg.Referent.withNodeAndTokens(value.log.id, tokens);
				output.log = tg.Referent.toDataString(referent, (id) => id);
			}
			if (value.output !== undefined) {
				output.output = tg.Value.toData(value.output);
			}
			if (value.retry) {
				output.retry = value.retry;
			}
			if (value.startedAt !== null) {
				output.started_at = value.startedAt;
			}
			if (value.stderr !== "inherit") {
				output.stderr = value.stderr;
			}
			if (value.stdin !== "inherit") {
				output.stdin = value.stdin;
			}
			if (value.stdout !== "inherit") {
				output.stdout = value.stdout;
			}
			if (value.tty !== null) {
				output.tty = value.tty;
			}
			return output;
		};

		export let fromData = (data: tg.Process.Data): tg.Process.State => {
			let output: State = {
				actualChecksum: data.actual_checksum ?? null,
				cacheable: data.cacheable ?? false,
				children:
					data.children !== undefined && data.children !== null
						? data.children.map(tg.Process.Child.fromData)
						: null,
				command: tg.Command.withId(data.command),
				createdAt: data.created_at,
				debug: data.debug ?? null,
				error:
					data.error !== undefined && data.error !== null
						? typeof data.error === "string"
							? tg.Error.withReferent(
									tg.Referent.fromDataString(
										data.error,
										(id) => id as tg.Error.Id,
									),
								)
							: tg.Error.fromData(data.error)
						: null,
				exit: data.exit ?? null,
				expectedChecksum: data.expected_checksum ?? null,
				finishedAt: data.finished_at ?? null,
				host: data.host,
				log:
					data.log !== undefined && data.log !== null
						? (() => {
								let referent = tg.Referent.fromDataString(
									data.log,
									(id) => id as tg.Blob.Id,
								);
								return tg.Blob.withReferent(referent);
							})()
						: null,
				retry: data.retry ?? false,
				sandbox: data.sandbox,
				startedAt: data.started_at ?? null,
				status: data.status,
				stderr: data.stderr ?? "inherit",
				stdin: data.stdin ?? "inherit",
				stdout: data.stdout ?? "inherit",
				tty: data.tty ?? null,
			};
			if (data.output !== undefined) {
				output.output = tg.Value.fromData(data.output);
			}
			return output;
		};
	}

	/** A mount. */
	export type Mount = tg.Sandbox.Mount;

	export type Tty = {
		size: tg.Process.Tty.Size;
	};

	export namespace Tty {
		export type Size = {
			cols: number;
			rows: number;
		};
	}

	export import Stdio = stdio.Stdio;

	export type Signal = (typeof Signal)[keyof typeof Signal];

	export let Signal = {
		ABRT: "ABRT",
		ALRM: "ALRM",
		FPE: "FPE",
		HUP: "HUP",
		ILL: "ILL",
		INT: "INT",
		KILL: "KILL",
		PIPE: "PIPE",
		QUIT: "QUIT",
		SEGV: "SEGV",
		TERM: "TERM",
		USR1: "USR1",
		USR2: "USR2",
	} as const;

	export type Status = "started" | "finished";

	export type Data = {
		actual_checksum?: tg.Checksum | null;
		cacheable?: boolean;
		children?: Array<tg.Process.Data.Child> | null;
		command: tg.Command.Id;
		created_at: number;
		debug?: tg.Process.Debug | null;
		error?: tg.Error.Data | string | null;
		exit?: number | null;
		expected_checksum?: tg.Checksum | null;
		finished_at?: number | null;
		host: string;
		log?: string | null;
		output?: tg.Value.Data;
		retry?: boolean;
		sandbox: string;
		started_at?: number | null;
		status: tg.Process.Status;
		stderr?: tg.Process.Stdio;
		stdin?: tg.Process.Stdio;
		stdout?: tg.Process.Stdio;
		tty?: tg.Process.Tty | null;
	};

	export namespace Data {
		export type Child = {
			cached?: boolean;
			process: string;
		};

		export let withoutTokens = (data: tg.Process.Data): tg.Process.Data => {
			let output = { ...data };
			if (data.children !== undefined && data.children !== null) {
				output.children = data.children.map((child) => {
					let referent = tg.Referent.fromDataString(
						child.process,
						(id) => id as tg.Process.Id,
					);
					return {
						...child,
						process: tg.Referent.toDataString(
							tg.Referent.withoutToken(referent),
							(id) => id,
						),
					};
				});
			}
			if (data.error !== undefined && data.error !== null) {
				if (typeof data.error === "string") {
					let referent = tg.Referent.fromDataString(
						data.error,
						(id) => id as tg.Error.Id,
					);
					output.error = tg.Referent.toDataString(
						tg.Referent.withoutToken(referent),
						(id) => id,
					);
				} else {
					output.error = tg.Error.Data.withoutTokens(data.error);
				}
			}
			if (data.log !== undefined && data.log !== null) {
				let referent = tg.Referent.fromDataString(
					data.log,
					(id) => id as tg.Blob.Id,
				);
				output.log = tg.Referent.toDataString(
					tg.Referent.withoutToken(referent),
					(id) => id,
				);
			}
			if (data.output !== undefined) {
				output.output = tg.Value.Data.withoutTokens(data.output);
			}
			return output;
		};
	}

	export type Wait = {
		error: tg.Error | null;
		exit: number;
		output?: tg.Value;
	};

	export namespace Wait {
		export type Arg = {
			lease?: string | null;
			location?: tg.Location.Arg | null;
			tokens?: tg.Authorization.Tokens | null;
		};

		export type Data = {
			error?: tg.Error.Data | string | null;
			exit: number;
			output?: tg.Value.Data;
		};

		export let fromData = (data: tg.Process.Wait.Data): tg.Process.Wait => {
			let output: Wait = {
				error:
					data.error !== undefined && data.error !== null
						? typeof data.error === "string"
							? tg.Error.withReferent(
									tg.Referent.fromDataString(
										data.error,
										(id) => id as tg.Error.Id,
									),
								)
							: tg.Error.fromData(data.error)
						: null,
				exit: data.exit,
			};
			if ("output" in data) {
				output.output = tg.Value.fromData(data.output);
			}
			return output;
		};

		export let inheritTokens = (
			wait: tg.Process.Wait,
			tokens: tg.Authorization.Tokens,
		): void => {
			if (wait.error !== null) {
				tg.Object.inheritTokens(wait.error, tokens);
			}
			if (wait.output !== undefined) {
				tg.Value.inheritTokens(wait.output, tokens);
			}
		};

		export let toData = (value: Wait): Data => {
			let output: Data = {
				exit: value.exit,
			};
			if (value.error !== null) {
				output.error = tg.Error.toDataOrId(value.error);
			}
			if (value.output !== undefined) {
				output.output = tg.Value.toData(value.output);
			}
			return output;
		};
	}
}

async function isJsProcessBuilderArg(
	args: tg.Args<tg.Process.Arg>,
): Promise<boolean> {
	let args_ = await Promise.all(args.map(tg.resolve));
	for (let arg of args_) {
		let command: tg.Command | undefined;
		if (arg instanceof tg.Command) {
			command = arg;
		} else if (
			typeof arg === "object" &&
			arg !== null &&
			"command" in arg &&
			arg.command !== undefined &&
			arg.command !== null
		) {
			let command_ = arg.command;
			let node =
				typeof command_ === "object" && command_ !== null && "node" in command_
					? command_.node
					: command_;
			if (node instanceof tg.Command) {
				command = node;
			}
		}
		if (
			command !== undefined &&
			tg.Command.Object.isJs(await command.object())
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
