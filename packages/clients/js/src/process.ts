import * as tg from "./index.ts";
import * as build from "./process/build.ts";
import * as exec from "./process/exec.ts";
import * as run from "./process/run.ts";
import * as spawn from "./process/spawn.ts";
import * as stdio from "./process/stdio.ts";
import type { Get as ProcessGet } from "./client/process/get.ts";
import type { Put as ProcessPut } from "./client/process/put.ts";

export let process: {
	args: Array<tg.Value>;
	cwd: string;
	env: { [key: string]: tg.Value };
	executable: tg.Command.Executable;
} = {} as any;

export let setProcess = (newProcess: typeof process) => {
	Object.defineProperties(
		process,
		Object.getOwnPropertyDescriptors(newProcess),
	);
};

export class Process<O extends tg.Value = tg.Value> {
	#id: number | tg.Process.Id;
	#lease: string | undefined;
	#location: tg.Location.Arg | undefined;
	#options: tg.Referent.Options;
	#promise: Promise<tg.Process.Wait> | undefined;
	#state: tg.Process.State | undefined;
	#stderr: tg.Process.Stdio.Reader;
	#stdin: tg.Process.Stdio.Writer;
	#stdioPromise: Promise<void> | undefined;
	#stdout: tg.Process.Stdio.Reader;
	#token: tg.Grant.Token | undefined;
	#wait: tg.Process.Wait | undefined;

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
	): Promise<tg.Process.ArgObject> {
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
					let host = tg.host.current;
					let executable =
						typeof tg.process.env.SHELL === "string"
							? tg.process.env.SHELL
							: "sh";
					return {
						args: ["-c", arg],
						executable,
						host,
					};
				} else if (arg instanceof tg.Command) {
					let object = await arg.object();
					let output: tg.Process.ArgObject = {
						args: object.args,
						env: object.env,
						executable: object.executable,
						host: object.host,
					};
					if (object.cwd !== undefined) {
						output.cwd = object.cwd;
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
			},
			reduce: {
				args: "append",
				env: "merge",
				mounts: "append",
				ports: "append",
			},
		});
	}

	static async spawnArg(...args: tg.Args<tg.Process.Arg>): Promise<{
		arg: tg.Spawn.Arg;
		options: tg.Referent.Options;
	}> {
		return await spawn.spawnArg(...args);
	}

	static async execUnsandboxed(arg: tg.Spawn.Arg): Promise<never> {
		return await exec.execUnsandboxed(arg);
	}

	static async spawnUnsandboxed<O extends tg.Value = tg.Value>(
		arg: tg.Spawn.Arg,
		options?: tg.Referent.Options,
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
		tempPath: string,
		outputPath: string,
	): Promise<tg.Process.Wait> {
		return await spawn.waitUnsandboxed(pid, stdio, tempPath, outputPath);
	}

	static async prepareUnsandboxedCommand(
		arg: tg.Spawn.Arg,
		outputPath?: string,
	): Promise<tg.Process.PreparedUnsandboxedCommandOutput> {
		return await spawn.prepareUnsandboxedCommand(arg, outputPath);
	}

	static async spawnSandboxed<O extends tg.Value = tg.Value>(
		arg: tg.Spawn.Arg,
		options?: tg.Referent.Options,
	): Promise<tg.Process<O>> {
		return await spawn.spawnSandboxed<O>(arg, options);
	}

	constructor(arg: tg.Process.ConstructorArg) {
		this.#id = arg.id;
		this.#lease = arg.lease;
		this.#location = arg.location;
		this.#options = arg.options ?? {};
		this.#state = arg.state;
		this.#stdioPromise = arg.stdioPromise;
		this.#promise = arg.promise;
		this.#stdin = arg.stdin;
		this.#stdout = arg.stdout;
		this.#stderr = arg.stderr;
		this.#token = arg.token;
		this.#wait = arg.wait;
		this.#stdin.setProcess(this);
		this.#stdout.setProcess(this);
		this.#stderr.setProcess(this);
	}

	get state(): tg.Process.State | undefined {
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
		let output = await tg.client.getProcess(this.#id, {
			location: this.#location,
		});
		if (output.location !== undefined) {
			this.#location = tg.Location.Arg.fromLocation(output.location);
		}
		this.#state = tg.Process.State.fromData(output.data);
		tg.Process.State.inheritToken(this.#state, this.#token);
	}

	/** Reload the process's state. */
	async reload(): Promise<void> {
		await this.load();
	}

	async #getSandbox(): Promise<tg.Sandbox.Get.Output | undefined> {
		if (typeof this.#id === "number") {
			return undefined;
		}
		await this.load();
		let sandbox = this.#state!.sandbox;
		if (sandbox === undefined) {
			return undefined;
		}
		return await tg.client.getSandbox(sandbox);
	}

	/** Get this process's ID. */
	get id(): number | tg.Process.Id {
		return this.#id;
	}

	/** Get this process's location arg. */
	get location(): tg.Location.Arg | undefined {
		return this.#location;
	}

	get token(): tg.Grant.Token | undefined {
		return this.#token;
	}

	set token(token: tg.Grant.Token | undefined) {
		this.#token = token;
	}

	inheritToken(token: tg.Grant.Token | undefined): void {
		if (this.#token === undefined) {
			this.#token = token;
		}
	}

	/** Get this process's command. */
	get command(): Promise<tg.Command> {
		return (async () => {
			await this.load();
			let command = this.#state!.command;

			tg.Object.inheritToken(command, this.#token);

			return command;
		})();
	}

	/** Get this process's command's args. */
	get args(): Promise<Array<tg.Value>> {
		return (async () => {
			return await (
				await this.command
			).args;
		})();
	}

	/** Get this process's command's cwd. */
	get cwd(): Promise<string | undefined> {
		return (async () => {
			return await (
				await this.command
			).cwd;
		})();
	}

	/** Get this process's command's environment. */
	async env(): Promise<{ [key: string]: tg.Value }>;
	async env(name: string): Promise<tg.Value | undefined>;
	async env(
		name?: string,
	): Promise<{ [name: string]: tg.Value } | tg.Value | undefined> {
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
			return sandbox?.network !== undefined;
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
	get sandbox(): Promise<string | undefined> {
		return (async () => {
			if (typeof this.#id === "number") {
				return undefined;
			}
			await this.load();
			return this.#state!.sandbox;
		})();
	}

	/** Get this process's command's user. */
	get user(): Promise<string | undefined> {
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
	get lease(): string | undefined {
		return this.#lease;
	}

	/** Send a signal to this process. */
	async signal(signal: tg.Process.Signal): Promise<void> {
		if (typeof this.#id === "number") {
			await tg.host.signal(this.#id, signal);
			return;
		}
		let location = this.#location;
		if (location === undefined) {
			await this.load();
			location = this.#location;
		}
		let arg = {
			location,
			signal,
		};
		await tg.client.signalProcess(this.#id, arg);
	}

	/** Wait for this process to exit. */
	async wait(): Promise<tg.Process.Wait> {
		if (this.#stdioPromise !== undefined) {
			await this.#stdioPromise;
		}
		if (this.#wait !== undefined) {
			tg.Process.Wait.inheritToken(this.#wait, this.#token);
			return this.#wait;
		}
		if (typeof this.#id === "number") {
			tg.assert(this.#promise !== undefined);
			let wait = await this.#promise;
			tg.Process.Wait.inheritToken(wait, this.#token);
			this.#wait = wait;
			return wait;
		}
		let arg: tg.Process.Wait.Arg = {
			lease: this.#lease,
			location: this.#location,
			token: this.#token,
		};
		let promise = await tg.client.waitProcessPromise(this.#id, arg);
		let wait = await promise();
		if (wait === undefined) {
			throw new Error("failed to find the process");
		}
		tg.Process.Wait.inheritToken(wait, this.#token);
		this.#wait = wait;
		return wait;
	}

	/** Wait for this process to exit and return the output. */
	async output(): Promise<O> {
		let wait = await this.wait();

		if (wait.error !== undefined) {
			let error = wait.error;
			const source = {
				item: error,
				options: this.#options,
			};
			const values: { [key: string]: string } = {
				id: String(this.id),
			};
			if (this.#options.name !== undefined) {
				values.name = this.#options.name;
			}
			throw tg.error("the child process failed", {
				source,
				values,
			});
		}
		if (wait.exit >= 1 && wait.exit < 128) {
			const error = tg.error(`the process exited with code ${wait.exit}`);
			const source = {
				item: error,
				options: this.#options,
			};
			const values: { [key: string]: string } = {
				id: String(this.id),
			};
			if (this.#options.name !== undefined) {
				values.name = this.#options.name;
			}
			throw tg.error("the child process failed", {
				source,
				values,
			});
		}
		if (wait.exit >= 128) {
			const error = tg.error(`the process exited with code ${wait.exit}`);
			const source = {
				item: error,
				options: this.#options,
			};
			const values: { [key: string]: string } = {
				id: String(this.id),
			};
			if (this.#options.name !== undefined) {
				values.name = this.#options.name;
			}
			throw tg.error(
				`the child process exited with signal ${wait.exit - 128}`,
				{
					source,
					values,
				},
			);
		}

		let output = wait.output;

		tg.Value.inheritToken(output, this.#token);

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
		if (location === undefined) {
			await this.load();
			location = this.#location;
		}
		await tg.client.setProcessTtySize(this.#id, {
			location,
			size,
		});
	}
}

export namespace Process {
	export type Id = string;

	export namespace Get {
		export type Arg = ProcessGet.Arg;

		export type Output = ProcessGet.Output;
	}

	export namespace Put {
		export type Arg = ProcessPut.Arg;

		export type Output = ProcessPut.Output;
	}

	export namespace Tty {
		export namespace Put {
			export type Arg = {
				location?: tg.Location.Arg | undefined;
				size: tg.Process.Tty.Size;
			};
		}
	}

	export interface Builder<
		M extends tg.Process.Builder.Mode,
		A extends Array<tg.Value> = Array<tg.Value>,
		O extends tg.Value = tg.Value,
	> {
		(...args: tg.UnresolvedArgs<A>): tg.Process.Builder<M, [], O>;
	}

	export class Builder<
		M extends tg.Process.Builder.Mode,
		A extends Array<tg.Value> = Array<tg.Value>,
		O extends tg.Value = tg.Value,
	> extends Function {
		#args: tg.Args<tg.Process.Arg>;
		#mode: M;
		#validate?: (arg: tg.Process.ArgObject) => void;

		constructor(mode: M, ...args: tg.Args<tg.Process.Arg>) {
			super();
			this.#args = args;
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

		arg(...args: Array<tg.Unresolved<tg.Value>>): this {
			this.#args.push({ args });
			return this;
		}

		args(
			...args: Array<tg.Unresolved<tg.MaybeMutation<Array<tg.Value>>>>
		): this {
			this.#args.push(...args.map((args) => ({ args })));
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

		debug(
			debug: tg.Unresolved<
				tg.MaybeMutation<boolean | tg.Process.Debug | undefined>
			> = true,
		): this {
			this.#args.push({ debug });
			return this;
		}

		cpu(cpu: tg.Unresolved<tg.MaybeMutation<number>>): this {
			this.#args.push({ cpu });
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

		memory(memory: tg.Unresolved<tg.MaybeMutation<number>>): this {
			this.#args.push({ memory });
			return this;
		}

		mount(...mounts: Array<tg.Unresolved<tg.Sandbox.Mount>>): this {
			this.#args.push({ mounts });
			return this;
		}

		mounts(
			...mounts: Array<tg.Unresolved<tg.MaybeMutation<Array<tg.Sandbox.Mount>>>>
		): this {
			this.#args.push(...mounts.map((mounts) => ({ mounts })));
			return this;
		}

		named(name: tg.Unresolved<tg.MaybeMutation<string | undefined>>): this {
			this.#args.push({ name });
			return this;
		}

		network(
			network?: tg.Unresolved<
				tg.MaybeMutation<boolean | tg.Sandbox.Network | undefined>
			>,
		): this {
			this.#args.push({ network: network ?? true });
			return this;
		}

		port(...ports: Array<tg.Unresolved<tg.Sandbox.Port>>): this {
			this.#args.push({ ports });
			return this;
		}

		ports(
			...ports: Array<tg.Unresolved<tg.MaybeMutation<Array<tg.Sandbox.Port>>>>
		): this {
			this.#args.push(...ports.map((ports) => ({ ports })));
			return this;
		}

		sandbox(
			sandbox?: tg.Unresolved<
				tg.MaybeMutation<boolean | tg.Sandbox.Arg | tg.Sandbox.Id | undefined>
			>,
		): this {
			this.#args.push({ sandbox: sandbox ?? true });
			return this;
		}

		stderr(stderr: tg.Unresolved<tg.MaybeMutation<tg.Process.Stdio>>): this {
			this.#args.push({ stderr });
			return this;
		}

		stdin(
			stdin: tg.Unresolved<tg.MaybeMutation<tg.Blob.Arg | tg.Process.Stdio>>,
		): this {
			this.#args.push({ stdin });
			return this;
		}

		stdio(stdio: tg.Unresolved<tg.MaybeMutation<tg.Process.Stdio>>): this {
			this.#args.push({ stdin: stdio, stdout: stdio, stderr: stdio });
			return this;
		}

		stdout(stdout: tg.Unresolved<tg.MaybeMutation<tg.Process.Stdio>>): this {
			this.#args.push({ stdout });
			return this;
		}

		tty(
			tty: tg.Unresolved<
				tg.MaybeMutation<boolean | tg.Process.Tty | undefined>
			>,
		): this {
			this.#args.push({ tty });
			return this;
		}

		validate(validate: (arg: tg.Process.ArgObject) => void): this {
			this.#validate = validate;
			return this;
		}

		exec(): tg.Process.Builder<"exec", A, never> {
			let output = new tg.Process.Builder("exec", ...this.#args);
			if (this.#validate !== undefined) {
				output.validate(this.#validate);
			}
			return output as tg.Process.Builder<"exec", A, never>;
		}

		run(): tg.Process.Builder<"run", A, O> {
			let output = new tg.Process.Builder("run", ...this.#args);
			if (this.#validate !== undefined) {
				output.validate(this.#validate);
			}
			return output as tg.Process.Builder<"run", A, O>;
		}

		spawn(): tg.Process.Builder<"spawn", A, O> {
			let output = new tg.Process.Builder("spawn", ...this.#args);
			if (this.#validate !== undefined) {
				output.validate(this.#validate);
			}
			return output as tg.Process.Builder<"spawn", A, O>;
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
			let output = await tg.Process.spawnArg(...this.#args);
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
	}

	export namespace Builder {
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
		lease?: string | undefined;
		location?: tg.Location.Arg | undefined;
		options?: tg.Referent.Options;
		promise?: Promise<tg.Process.Wait> | undefined;
		state?: State | undefined;
		stderr: tg.Process.Stdio.Reader;
		stdin: tg.Process.Stdio.Writer;
		stdioPromise?: Promise<void> | undefined;
		stdout: tg.Process.Stdio.Reader;
		token?: tg.Grant.Token | undefined;
		wait?: tg.Process.Wait | undefined;
	};

	export type PreparedUnsandboxedCommandOutput = {
		args: Array<string>;
		cwd?: string | undefined;
		env: { [key: string]: string };
		executable: string;
		outputPath: string;
		tempPath: string;
	};

	export type Arg =
		| undefined
		| string
		| tg.Artifact
		| tg.Template
		| tg.Command
		| ArgObject;

	export type ArgObject = {
		/** The command's arguments. */
		args?: Array<tg.Value> | undefined;

		/** The cache location arg. */
		cache_location?: tg.Location.Arg | undefined;

		/** If a checksum of the process's output is provided, then the process can be cached even if it is not sandboxed. */
		checksum?: tg.Checksum | undefined;

		/** The base command. */
		command?: tg.MaybeReferent<tg.Command> | undefined;

		/** The sandbox's CPU allocation. */
		cpu?: number | undefined;

		/** The command's working directory. */
		cwd?: string | undefined;

		/** Configure debugging. */
		debug?: boolean | tg.Process.Debug | undefined;

		/** The command's environment. */
		env?: tg.MaybeMutationMap | undefined;

		/** The command's executable. */
		executable?: tg.Command.Arg.Executable | undefined;

		/** The command's host. */
		host?: string | undefined;

		/** The process location arg. */
		location?: tg.Location.Arg | undefined;

		/** The sandbox's memory allocation. */
		memory?: number | undefined;

		/** Configure mounts. */
		mounts?: Array<tg.Sandbox.Mount> | undefined;

		/** The process's name. */
		name?: string | undefined;

		/** Configure network. */
		network?: boolean | tg.Sandbox.Network | undefined;

		/** Configure port forwarding. */
		ports?: Array<tg.Sandbox.Port> | undefined;

		/** Configure or select the sandbox for this process. */
		sandbox?: boolean | tg.Sandbox.Arg | tg.Sandbox.Id | undefined;

		/** Configure stderr. */
		stderr?: tg.Process.Stdio | undefined;

		/** Configure stdin, or set it to a blob. */
		stdin?: tg.Blob.Arg | tg.Process.Stdio | undefined;

		/** Configure stdout. */
		stdout?: tg.Process.Stdio | undefined;

		/** Configure whether the process should allocate a tty. */
		tty?: boolean | tg.Process.Tty | undefined;

		/** The command's user. */
		user?: string | undefined;
	};

	export type State = {
		actualChecksum?: tg.Checksum | undefined;
		cacheable: boolean;
		children?: Array<tg.Process.Child> | undefined;
		command: tg.Command;
		createdAt: number;
		debug?: tg.Process.Debug | undefined;
		error: tg.Error | undefined;
		exit: number | undefined;
		expectedChecksum?: tg.Checksum | undefined;
		finishedAt?: number | undefined;
		host: string;
		log?: tg.Blob | undefined;
		output?: tg.Value;
		retry: boolean;
		sandbox?: string;
		startedAt?: number | undefined;
		status: tg.Process.Status;
		stderr: tg.Process.Stdio;
		stdin: tg.Process.Stdio;
		stdout: tg.Process.Stdio;
		tty?: tg.Process.Tty | undefined;
	};

	export type Child = {
		cached: boolean;
		options: tg.Referent.Options;
		process: tg.Process;
	};

	export type Debug = {
		addr?: string | undefined;
		mode?: tg.Process.Debug.Mode | undefined;
	};

	export namespace Debug {
		export type Mode = "normal" | "break" | "wait";
	}

	export namespace Child {
		export let toData = (value: tg.Process.Child): tg.Process.Data.Child => {
			let options: tg.Referent.Data.Options = {};
			if (value.options.artifact !== undefined) {
				options.artifact = value.options.artifact;
			}
			if (value.options.id !== undefined) {
				options.id = value.options.id;
			}
			if (value.options.location !== undefined) {
				options.location = tg.Location.Arg.toDataString(value.options.location);
			}
			if (value.options.name !== undefined) {
				options.name = value.options.name;
			}
			if (value.options.path !== undefined) {
				options.path = value.options.path;
			}
			if (value.options.tag !== undefined) {
				options.tag = value.options.tag;
			}
			let process = value.process.id;
			if (typeof process !== "string") {
				throw new Error("expected a sandboxed process id");
			}
			let token = value.process.token;
			return {
				cached: value.cached,
				options,
				process: token === undefined ? process : { id: process, token },
			};
		};

		export let fromData = (data: tg.Process.Data.Child): tg.Process.Child => {
			let options: tg.Referent.Options = {};
			if (data.options.artifact !== undefined) {
				options.artifact = data.options.artifact;
			}
			if (data.options.id !== undefined) {
				options.id = data.options.id;
			}
			if (data.options.location !== undefined) {
				options.location = tg.Location.Arg.fromDataString(
					data.options.location,
				);
			}
			if (data.options.name !== undefined) {
				options.name = data.options.name;
			}
			if (data.options.path !== undefined) {
				options.path = data.options.path;
			}
			if (data.options.tag !== undefined) {
				options.tag = data.options.tag;
			}
			return {
				cached: data.cached ?? false,
				options,
				process: new tg.Process({
					id: typeof data.process === "string" ? data.process : data.process.id,
					location: undefined,
					state: undefined,
					stderr: new tg.Process.Stdio.Reader({
						stream: "stderr",
					}),
					stdin: new tg.Process.Stdio.Writer({
						stream: "stdin",
					}),
					stdout: new tg.Process.Stdio.Reader({
						stream: "stdout",
					}),
					token:
						typeof data.process === "string" ? undefined : data.process.token,
				}),
			};
		};
	}

	export namespace State {
		export let inheritToken = (
			state: State,
			token: tg.Grant.Token | undefined,
		): void => {
			tg.Object.inheritToken(state.command, token);
			for (let child of state.children ?? []) {
				child.process.inheritToken(token);
			}
			if (state.error !== undefined) {
				tg.Object.inheritToken(state.error, token);
			}
			if (state.log !== undefined) {
				tg.Object.inheritToken(state.log, token);
			}
			if ("output" in state) {
				tg.Value.inheritToken(state.output, token);
			}
		};

		export let toData = (value: State): Data => {
			let output: Data = {
				command: value.command.id,
				created_at: value.createdAt,
				host: value.host,
				sandbox: value.sandbox!,
				status: value.status,
			};
			if (value.actualChecksum !== undefined) {
				output.actual_checksum = value.actualChecksum;
			}
			if (value.cacheable) {
				output.cacheable = value.cacheable;
			}
			if (value.children !== undefined) {
				output.children = value.children.map(tg.Process.Child.toData);
			}
			if (value.debug !== undefined) {
				output.debug = value.debug;
			}
			if (value.error !== undefined) {
				output.error = tg.Error.toDataOrId(value.error);
			}
			if (value.exit !== undefined) {
				output.exit = value.exit;
			}
			if (value.expectedChecksum !== undefined) {
				output.expected_checksum = value.expectedChecksum;
			}
			if (value.finishedAt !== undefined) {
				output.finished_at = value.finishedAt;
			}
			if (value.log !== undefined) {
				let token = value.log.state.token;
				output.log =
					token === undefined ? value.log.id : { id: value.log.id, token };
			}
			if ("output" in value) {
				output.output = tg.Value.toData(value.output);
			}
			if (value.retry) {
				output.retry = value.retry;
			}
			if (value.startedAt !== undefined) {
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
			if (value.tty !== undefined) {
				output.tty = value.tty;
			}
			return output;
		};

		export let fromData = (data: tg.Process.Data): tg.Process.State => {
			let output: State = {
				actualChecksum: data.actual_checksum,
				cacheable: data.cacheable ?? false,
				children:
					data.children !== undefined
						? data.children.map(tg.Process.Child.fromData)
						: undefined,
				command: tg.Command.withId(data.command),
				createdAt: data.created_at,
				debug: data.debug,
				error:
					data.error !== undefined
						? typeof data.error === "string" || "id" in data.error
							? (() => {
									let error =
										typeof data.error === "string"
											? tg.Error.withId(data.error)
											: tg.Error.withId(data.error.id);
									if (typeof data.error !== "string") {
										error.state.token = data.error.token;
									}
									return error;
								})()
							: tg.Error.fromData(data.error)
						: undefined,
				exit: data.exit,
				expectedChecksum: data.expected_checksum,
				finishedAt: data.finished_at,
				host: data.host,
				log:
					data.log !== undefined
						? (() => {
								let blob =
									typeof data.log === "string"
										? tg.Blob.withId(data.log)
										: tg.Blob.withId(data.log.id);
								if (typeof data.log !== "string") {
									blob.state.token = data.log.token;
								}
								return blob;
							})()
						: undefined,
				retry: data.retry ?? false,
				sandbox: data.sandbox,
				startedAt: data.started_at,
				status: data.status,
				stderr: data.stderr ?? "inherit",
				stdin: data.stdin ?? "inherit",
				stdout: data.stdout ?? "inherit",
				tty: data.tty,
			};
			if ("output" in data) {
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

	export type Status = "created" | "dequeued" | "started" | "finished";

	export type Data = {
		actual_checksum?: tg.Checksum;
		cacheable?: boolean;
		children?: Array<tg.Process.Data.Child>;
		command: tg.Command.Id;
		created_at: number;
		debug?: tg.Process.Debug;
		error?: tg.Error.Data | tg.Grant.MaybeWithToken<tg.Error.Id>;
		exit?: number;
		expected_checksum?: tg.Checksum;
		finished_at?: number;
		host: string;
		log?: tg.Grant.MaybeWithToken<tg.Blob.Id>;
		output?: tg.Value.Data;
		retry?: boolean;
		sandbox: string;
		started_at?: number;
		status: tg.Process.Status;
		stderr?: tg.Process.Stdio;
		stdin?: tg.Process.Stdio;
		stdout?: tg.Process.Stdio;
		tty?: tg.Process.Tty;
	};

	export namespace Data {
		export type Child = {
			cached?: boolean;
			options: tg.Referent.Data.Options;
			process: tg.Grant.MaybeWithToken<tg.Process.Id>;
		};

		export let removeTokens = (data: tg.Process.Data): tg.Process.Data => {
			if (data.children !== undefined) {
				for (let child of data.children) {
					if (typeof child.process !== "string") {
						child.process = child.process.id;
					}
				}
			}
			if (
				data.error !== undefined &&
				typeof data.error !== "string" &&
				"id" in data.error
			) {
				data.error = data.error.id;
			}
			if (data.log !== undefined && typeof data.log !== "string") {
				data.log = data.log.id;
			}
			if ("output" in data) {
				tg.Value.Data.removeTokens(data.output);
			}
			return data;
		};
	}

	export type Wait = {
		error: tg.Error | undefined;
		exit: number;
		output?: tg.Value;
	};

	export namespace Wait {
		export type Arg = {
			lease: string | undefined;
			location?: tg.Location.Arg | undefined;
			token?: tg.Grant.Token | undefined;
		};

		export type Data = {
			error?: tg.Error.Data | tg.Error.Id;
			exit: number;
			output?: tg.Value.Data;
		};

		export let fromData = (data: tg.Process.Wait.Data): tg.Process.Wait => {
			let output: Wait = {
				error:
					data.error !== undefined
						? typeof data.error === "string"
							? tg.Error.withId(data.error)
							: tg.Error.fromData(data.error)
						: undefined,
				exit: data.exit,
			};
			if ("output" in data) {
				output.output = tg.Value.fromData(data.output);
			}
			return output;
		};

		export let inheritToken = (
			wait: tg.Process.Wait,
			token: tg.Grant.Token | undefined,
		): void => {
			if (wait.error !== undefined) {
				tg.Object.inheritToken(wait.error, token);
			}
			if ("output" in wait) {
				tg.Value.inheritToken(wait.output, token);
			}
		};

		export let toData = (value: Wait): Data => {
			let output: Data = {
				exit: value.exit,
			};
			if (value.error !== undefined) {
				output.error = tg.Error.toData(value.error);
			}
			if ("output" in value) {
				output.output = tg.Value.toData(value.output);
			}
			return output;
		};
	}
}
