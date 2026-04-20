import * as tg from "./index.ts";

export let process: {
	args: Array<tg.Value>;
	cwd: string;
	env: { [key: string]: tg.Value };
	executable: tg.Command.Executable;
} = {} as any;

export let setProcess = (newProcess: typeof process) => {
	Object.assign(process, newProcess);
};

export class Process<O extends tg.Value = tg.Value> {
	#id: tg.Process.Id;
	#location: tg.Location.Arg | undefined;
	#options: tg.Referent.Options;
	#pid: number | undefined;
	#promise: Promise<tg.Process.Wait> | undefined;
	#state: tg.Process.State | undefined;
	#stderr: tg.Process.Stdio.Reader;
	#stdin: tg.Process.Stdio.Writer;
	#stdioPromise: Promise<void> | undefined;
	#stdout: tg.Process.Stdio.Reader;
	#token: string | undefined;
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
		let validate = (arg: tg.Process.ArgObject): void => {
			let sandbox = normalizeSandbox(arg);
			let cacheable =
				sandbox !== undefined &&
				typeof sandbox !== "string" &&
				(sandbox.mounts?.length ?? 0) === 0 &&
				(sandbox.network ?? false) === false &&
				arg.stdin === "null" &&
				arg.stdout === "log" &&
				arg.stderr === "log" &&
				(arg.tty === undefined || arg.tty === false);
			cacheable = cacheable || arg.checksum !== undefined;
			if (!cacheable) {
				throw tg.error("a build must be cacheable");
			}
		};
		let firstArg: tg.Process.ArgObject = {
			sandbox: true,
			stderr: "log",
			stdin: "null",
			stdout: "log",
			tty: false,
			env: {
				TANGRAM_HOST: tg.process.env.TANGRAM_HOST,
			},
		};
		if (typeof args[0] === "function") {
			return new tg.Process.Builder("run", firstArg, {
				host: "js",
				executable: tg.Command.Executable.fromData(tg.host.magic(args[0])),
				args: args.slice(1),
			}).validate(validate);
		} else if (Array.isArray(args[0]) && "raw" in args[0]) {
			let strings = args[0] as TemplateStringsArray;
			let placeholders = args.slice(1);
			let template = tg.template(strings, ...placeholders);
			let executable = tg.process.env.SHELL ?? "sh";
			tg.assert(tg.Command.Arg.Executable.is(executable));
			return new tg.Process.Builder("run", firstArg, {
				executable,
				args: ["-c", template],
			}).validate(validate);
		} else {
			return new tg.Process.Builder("run", firstArg, ...args).validate(
				validate,
			);
		}
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
		if (typeof args[0] === "function") {
			return new tg.Process.Builder("run", {
				host: "js",
				executable: tg.Command.Executable.fromData(tg.host.magic(args[0])),
				args: args.slice(1),
			});
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
			return new tg.Process.Builder("run", arg);
		} else {
			return new tg.Process.Builder("run", ...args);
		}
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
		if (typeof args[0] === "function") {
			return new tg.Process.Builder("spawn", {
				host: "js",
				executable: tg.Command.Executable.fromData(tg.host.magic(args[0])),
				args: args.slice(1),
			});
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
			return new tg.Process.Builder("spawn", arg);
		} else {
			return new tg.Process.Builder("spawn", ...args);
		}
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
					let host = tg.process.env.TANGRAM_HOST;
					tg.assert(host !== undefined, "TANGRAM_HOST must be set");
					let executable = tg.process.env.SHELL ?? "sh";
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
			},
		});
	}

	static async new<O extends tg.Value = tg.Value>(
		...args: tg.Args<tg.Process.Arg>
	): Promise<tg.Process<O>> {
		let arg = await tg.Process.arg(...args);

		let sandbox = normalizeSandbox(arg);

		if (sandbox === undefined) {
			let cwd = tg.process.cwd;
			let env = { ...tg.process.env };
			delete env.TANGRAM_OUTPUT;
			arg = await tg.Process.arg({ cwd, env }, arg);
		} else {
			if (!("host" in arg)) {
				arg.host = tg.process.env.TANGRAM_HOST as string | undefined;
			}
			if (arg.executable === tg.process.env.SHELL) {
				arg.executable = "sh";
			}
		}

		let command_: tg.Command | undefined;
		let options: tg.Referent.Options = {};
		if ("command" in arg) {
			if (
				typeof arg.command === "object" &&
				arg.command !== null &&
				"item" in arg.command
			) {
				command_ = tg.Command.expect(arg.command.item);
				options = { ...arg.command.options };
			} else {
				command_ = arg.command;
			}
		}
		if ("name" in arg) {
			options.name = arg.name;
		}
		let executable: tg.Command.Arg.Executable | undefined;
		if ("executable" in arg) {
			if (
				typeof arg.executable === "object" &&
				!tg.Artifact.is(arg.executable) &&
				"module" in arg.executable
			) {
				options = {
					...arg.executable.module.referent.options,
					...options,
				};
				executable = {
					...arg.executable,
					module: {
						...arg.executable.module,
						referent: {
							...arg.executable.module.referent,
							options: {},
						},
					},
				};
			} else {
				executable = arg.executable;
			}
		}

		let checksum = arg.checksum;
		let processStdin: tg.Process.Stdio | undefined;
		let commandStdin: tg.Blob.Arg | undefined;
		if ("stdin" in arg) {
			if (
				arg.stdin === "inherit" ||
				arg.stdin === "log" ||
				arg.stdin === "null" ||
				arg.stdin === "pipe" ||
				arg.stdin === "tty"
			) {
				processStdin = arg.stdin;
			} else if (arg.stdin !== undefined) {
				commandStdin = arg.stdin;
			}
		}
		let stdout = "stdout" in arg ? arg.stdout : undefined;
		let stderr = "stderr" in arg ? arg.stderr : undefined;
		let tty = "tty" in arg ? arg.tty : undefined;

		let command = await tg.command(
			command_,
			"args" in arg ? { args: arg.args } : undefined,
			"cwd" in arg ? { cwd: arg.cwd } : undefined,
			"env" in arg ? { env: arg.env } : undefined,
			executable !== undefined ? { executable: executable } : undefined,
			"host" in arg ? { host: arg.host } : undefined,
			"user" in arg ? { user: arg.user } : undefined,
			commandStdin !== undefined ? { stdin: commandStdin } : undefined,
		);

		let commandId = await command.store();
		let commandReferent = {
			item: commandId,
			options: options,
		};

		let spawnArg: tg.Handle.SpawnArg = {
			cache_location: arg.cache_location,
			checksum,
			command: commandReferent,
			location: arg.location,
			retry: false,
			sandbox,
			stderr: stderr ?? "inherit",
			stdin: processStdin ?? "inherit",
			stdout: stdout ?? "inherit",
			tty,
		};
		let process: tg.Process<O>;
		if (sandbox === undefined) {
			process = await this.spawnUnsandboxed<O>(spawnArg);
		} else {
			process = await this.spawnSandboxed<O>(spawnArg);
		}
		process.#options = options;

		return process;
	}

	static async spawnUnsandboxed<O extends tg.Value = tg.Value>(
		arg: tg.Handle.SpawnArg,
	): Promise<tg.Process<O>> {
		if (arg.tty !== undefined) {
			throw new Error("tty is not supported for unsandboxed processes");
		}
		if (arg.sandbox !== undefined) {
			throw new Error("sandboxing is not supported for unsandboxed processes");
		}
		if ((arg.stdin ?? "inherit").startsWith("blb_")) {
			throw new Error("blob stdin is not supported for unsandboxed processes");
		}

		let command = await tg.Command.withId(arg.command.item).object();
		if (command.stdin !== undefined) {
			throw new Error(
				"command stdin blobs are not supported for unsandboxed processes",
			);
		}
		if (command.user !== undefined) {
			throw new Error(
				"setting a user is not supported for unsandboxed processes",
			);
		}

		let id = tg.handle.processId();
		let tempPath = await tg.host.mkdtemp();
		let outputPath = tg.path.join(tempPath, id);
		let artifacts = await checkoutArtifacts(command);
		let env = await renderEnv(command.env, artifacts, outputPath);
		let { args, executable } = renderCommand(command, artifacts, outputPath);
		let spawnOutput = await tg.host.spawn({
			args,
			cwd: command.cwd,
			env,
			executable,
			stderr: renderStdio(arg.stderr ?? "inherit", "stderr"),
			stdin: renderStdio(arg.stdin ?? "inherit", "stdin"),
			stdout: renderStdio(arg.stdout ?? "inherit", "stdout"),
		});
		let stdin = new tg.Process.Stdio.Writer({
			fd: spawnOutput.stdin,
			unavailable: spawnOutput.stdin === undefined,
			stream: "stdin",
		});
		let stdout = new tg.Process.Stdio.Reader({
			fd: spawnOutput.stdout,
			unavailable: spawnOutput.stdout === undefined,
			stream: "stdout",
		});
		let stderr = new tg.Process.Stdio.Reader({
			fd: spawnOutput.stderr,
			unavailable: spawnOutput.stderr === undefined,
			stream: "stderr",
		});
		let pid = spawnOutput.pid;
		let promise = this.waitUnsandboxed(
			pid,
			{
				stderr,
				stdin,
				stdout,
			},
			tempPath,
			outputPath,
		);
		return new tg.Process<O>({
			id,
			location: undefined,
			pid,
			promise,
			state: undefined,
			stderr,
			stdin,
			token: undefined,
			stdout,
		});
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
		let wait: tg.Process.Wait | undefined;
		let waitError: unknown;
		try {
			let output = await tg.host.wait(pid);
			wait = {
				error: undefined,
				exit: output.exit,
			};
			let exists = await tg.host.exists(outputPath);
			if (exists) {
				let outputBytes = await tg.host.getxattr(
					outputPath,
					"user.tangram.output",
				);
				if (outputBytes !== undefined) {
					let tgon = tg.encoding.utf8.decode(outputBytes);
					wait.output = tg.Value.parse(tgon);
				}
				let errorBytes = await tg.host.getxattr(
					outputPath,
					"user.tangram.error",
				);
				if (errorBytes !== undefined) {
					let string = tg.encoding.utf8.decode(errorBytes);
					try {
						let value = tg.encoding.json.decode(string) as
							| tg.Error.Data
							| tg.Error.Id;
						wait.error =
							typeof value === "string"
								? tg.Error.withId(value)
								: tg.Error.fromData(value);
					} catch {
						wait.error = tg.Error.withId(string);
					}
				}
				if (wait.output === undefined) {
					let artifact = await tg.handle.checkin({
						options: {
							destructive: true,
							deterministic: true,
							ignore: false,
							lock: undefined,
							locked: true,
							root: true,
						},
						path: outputPath,
						updates: [],
					});
					wait.output = tg.Artifact.withId(artifact);
				}
			}
		} catch (error) {
			waitError = error;
		}
		try {
			for (let name of ["stdin", "stdout", "stderr"] as const) {
				await stdio[name].close();
			}
			await tg.host.remove(tempPath);
		} catch (error) {
			if (waitError === undefined) {
				waitError = error;
			}
		}
		if (waitError !== undefined) {
			throw waitError;
		}
		return wait!;
	}

	static async spawnSandboxed<O extends tg.Value = tg.Value>(
		arg: tg.Handle.SpawnArg,
	): Promise<tg.Process<O>> {
		let noTty = arg.tty === false;
		let provideStderr = arg.stderr === "pipe" || arg.stderr === "tty";
		let provideStdin = arg.stdin === "pipe" || arg.stdin === "tty";
		let provideStdout = arg.stdout === "pipe" || arg.stdout === "tty";
		let stdinIsTty = tg.host.isTty(0);
		let stdinIsForegroundControllingTty = tg.host.isForegroundControllingTty(0);
		let stdoutIsForegroundControllingTty =
			tg.host.isForegroundControllingTty(1);
		let stderrIsForegroundControllingTty =
			tg.host.isForegroundControllingTty(2);
		let hasForegroundTty =
			stdinIsForegroundControllingTty ||
			stdoutIsForegroundControllingTty ||
			stderrIsForegroundControllingTty;
		let resolveInheritedStdio = (
			stdio: string | undefined,
			foregroundTty: boolean,
			background: "pipe" | "null",
		): { local: "pipe" | "tty" | undefined; spawn: tg.Process.Stdio } => {
			let original = (stdio ?? "inherit") as tg.Process.Stdio;
			if (original !== "inherit") {
				return { local: undefined, spawn: original };
			}
			let spawn: tg.Process.Stdio =
				!noTty && foregroundTty ? "tty" : background;
			return {
				local: spawn === "null" ? undefined : (spawn as "pipe" | "tty"),
				spawn,
			};
		};
		let tty: tg.Process.Tty | undefined;
		if (arg.tty === true) {
			let size = tg.host.getTtySize();
			if (size !== undefined) {
				tty = { size };
			}
		} else if (arg.tty !== undefined && arg.tty !== false) {
			tty = arg.tty;
		}
		let { local: stdin, spawn: spawnStdin } = resolveInheritedStdio(
			arg.stdin,
			stdinIsForegroundControllingTty,
			stdinIsTty ? "null" : "pipe",
		);
		let { local: stdout, spawn: spawnStdout } = resolveInheritedStdio(
			arg.stdout,
			stdoutIsForegroundControllingTty,
			"pipe",
		);
		let { local: stderr, spawn: spawnStderr } = resolveInheritedStdio(
			arg.stderr,
			stderrIsForegroundControllingTty,
			"pipe",
		);
		if (
			tty === undefined &&
			(spawnStdin === "tty" || spawnStdout === "tty" || spawnStderr === "tty")
		) {
			let size = tg.host.getTtySize();
			if (size !== undefined) {
				tty = { size };
			}
		}
		let localTty = tty !== undefined && hasForegroundTty;
		if (
			tty !== undefined &&
			(spawnStdin === "tty" ||
				spawnStdout === "tty" ||
				spawnStderr === "tty") &&
			(tg.process.env.COLORTERM !== undefined ||
				tg.process.env.TERM !== undefined)
		) {
			let command = await tg.Command.withId(arg.command.item).object();
			let env = { ...command.env };
			let changed = false;
			for (let name of ["COLORTERM", "TERM"] as const) {
				let value = tg.process.env[name];
				if (
					value !== undefined &&
					!Object.prototype.hasOwnProperty.call(env, name)
				) {
					env[name] = value;
					changed = true;
				}
			}
			if (changed) {
				let commandId = await tg.Command.withObject({
					...command,
					env,
				}).store();
				arg.command.item = commandId;
			}
		}
		let output = await tg.handle.spawnProcess({
			...arg,
			retry: arg.retry ?? false,
			stderr: spawnStderr ?? "inherit",
			stdin: spawnStdin ?? "inherit",
			stdout: spawnStdout ?? "inherit",
			tty,
		});
		let wait =
			output.wait !== undefined
				? tg.Process.Wait.fromData(output.wait)
				: undefined;
		let location =
			output.location !== undefined
				? tg.Location.Arg.fromLocation(output.location)
				: undefined;
		let stdioPromise =
			stdin !== undefined ||
			stdout !== undefined ||
			stderr !== undefined ||
			localTty
				? stdioTask(output.process, location, stdin, stdout, stderr, localTty)
				: undefined;
		let process = new tg.Process<O>({
			id: output.process,
			location,
			state: undefined,
			stderr: new tg.Process.Stdio.Reader({
				unavailable: !provideStderr,
				stream: "stderr",
			}),
			stdin: new tg.Process.Stdio.Writer({
				unavailable: !provideStdin,
				stream: "stdin",
			}),
			stdioPromise,
			token: output.token,
			stdout: new tg.Process.Stdio.Reader({
				unavailable: !provideStdout,
				stream: "stdout",
			}),
			wait,
		});
		return process;
	}

	constructor(arg: tg.Process.ConstructorArg) {
		this.#id = arg.id;
		this.#location = arg.location;
		this.#options = arg.options ?? {};
		this.#state = arg.state;
		this.#stdioPromise = arg.stdioPromise;
		this.#pid = arg.pid;
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

	get pid(): number | undefined {
		return this.#pid;
	}

	static expect(value: unknown): tg.Process {
		tg.assert(value instanceof Process);
		return value;
	}

	static assert(value: unknown): asserts value is tg.Process {
		tg.assert(value instanceof Process);
	}

	async load(): Promise<void> {
		if (this.#pid !== undefined) {
			throw new Error("loading unsandboxed process state is not supported");
		}
		let output = await tg.handle.getProcess(this.#id, {
			location: this.#location,
		});
		if (output.location !== undefined) {
			this.#location = tg.Location.Arg.fromLocation(output.location);
		}
		this.#state = tg.Process.State.fromData(output.data);
	}

	async reload(): Promise<void> {
		await this.load();
	}

	async #getSandbox(): Promise<tg.Handle.SandboxGetOutput | undefined> {
		if (this.#pid !== undefined) {
			return undefined;
		}
		await this.load();
		let sandbox = this.#state!.sandbox;
		if (sandbox === undefined) {
			return undefined;
		}
		return await tg.handle.getSandbox(sandbox);
	}

	get id(): tg.Process.Id {
		return this.#id;
	}

	get location(): tg.Location.Arg | undefined {
		return this.#location;
	}

	get command(): Promise<tg.Command> {
		return (async () => {
			await this.load();
			return this.#state!.command;
		})();
	}

	get args(): Promise<Array<tg.Value>> {
		return (async () => {
			return await (
				await this.command
			).args;
		})();
	}

	get cwd(): Promise<string | undefined> {
		return (async () => {
			return await (
				await this.command
			).cwd;
		})();
	}

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
			return sandbox?.network ?? false;
		})();
	}

	get sandbox(): Promise<string | undefined> {
		return (async () => {
			if (this.#pid !== undefined) {
				return undefined;
			}
			await this.load();
			return this.#state!.sandbox;
		})();
	}

	get user(): Promise<string | undefined> {
		return (async () => {
			return await (
				await this.command
			).user;
		})();
	}

	get stdin(): tg.Process.Stdio.Writer {
		return this.#stdin;
	}

	get stdout(): tg.Process.Stdio.Reader {
		return this.#stdout;
	}

	get stderr(): tg.Process.Stdio.Reader {
		return this.#stderr;
	}

	async signal(signal: tg.Process.Signal): Promise<void> {
		if (this.#pid !== undefined) {
			await tg.host.signal(this.#pid, signal);
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
		await tg.handle.signalProcess(this.#id, arg);
	}

	async wait(): Promise<tg.Process.Wait> {
		if (this.#stdioPromise !== undefined) {
			await this.#stdioPromise;
		}
		if (this.#wait !== undefined) {
			return this.#wait;
		}
		if (this.#pid !== undefined) {
			tg.assert(this.#promise !== undefined);
			let wait = await this.#promise;
			this.#wait = wait;
			return wait;
		}
		let arg: tg.Handle.WaitArg = {
			location: this.#location,
			token: this.#token,
		};
		let data = await tg.handle.waitProcess(this.#id, arg);
		let wait = tg.Process.Wait.fromData(data);
		this.#wait = wait;
		return wait;
	}

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

		return output as O;
	}

	async setTtySize(size: tg.Process.Tty.Size): Promise<void> {
		if (this.#pid !== undefined) {
			throw new Error(
				"tty resizing is not supported for unsandboxed processes",
			);
		}
		let location = this.#location;
		if (location === undefined) {
			await this.load();
			location = this.#location;
		}
		await tg.handle.setProcessTtySize(this.#id, {
			location,
			size,
		});
	}
}

export namespace Process {
	export type Id = string;

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

		network(network: tg.Unresolved<tg.MaybeMutation<boolean>>): this {
			this.#args.push({ network });
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

		async #thenInner(): Promise<tg.Process.Builder.Output<M, O>> {
			let arg = await tg.Process.arg(...this.#args);
			this.#validate?.(arg);
			let process = await tg.Process.new<O>(arg);
			switch (this.#mode) {
				case "run": {
					return (await process.output()) as tg.Process.Builder.Output<M, O>;
				}
				case "spawn": {
					return process as tg.Process.Builder.Output<M, O>;
				}
			}
		}
	}

	export namespace Builder {
		export type Mode = "run" | "spawn";

		export type Output<
			M extends tg.Process.Builder.Mode,
			O extends tg.Value,
		> = M extends "spawn" ? tg.Process<O> : O;
	}

	export type ConstructorArg = {
		id: tg.Process.Id;
		location?: tg.Location.Arg | undefined;
		options?: tg.Referent.Options;
		pid?: number | undefined;
		promise?: Promise<tg.Process.Wait> | undefined;
		state?: State | undefined;
		stderr: tg.Process.Stdio.Reader;
		stdin: tg.Process.Stdio.Writer;
		stdioPromise?: Promise<void> | undefined;
		stdout: tg.Process.Stdio.Reader;
		token?: string | undefined;
		wait?: tg.Process.Wait | undefined;
	};

	export type Arg =
		| undefined
		| string
		| tg.Artifact
		| tg.Template
		| tg.Command
		| ArgObject;

	export type ArgObject = {
		args?: Array<tg.Value> | undefined;
		cache_location?: tg.Location.Arg | undefined;
		checksum?: tg.Checksum | undefined;
		command?: tg.MaybeReferent<tg.Command> | undefined;
		cpu?: number | undefined;
		cwd?: string | undefined;
		env?: tg.MaybeMutationMap | undefined;
		executable?: tg.Command.Arg.Executable | undefined;
		host?: string | undefined;
		location?: tg.Location.Arg | undefined;
		memory?: number | undefined;
		mounts?: Array<tg.Sandbox.Mount> | undefined;
		name?: string | undefined;
		network?: boolean | undefined;
		sandbox?: boolean | tg.Sandbox.Arg | tg.Sandbox.Id | undefined;
		stderr?: tg.Process.Stdio | undefined;
		stdin?: tg.Blob.Arg | tg.Process.Stdio | undefined;
		stdout?: tg.Process.Stdio | undefined;
		tty?: boolean | tg.Process.Tty | undefined;
		user?: string | undefined;
	};

	export type State = {
		actualChecksum?: tg.Checksum | undefined;
		cacheable: boolean;
		children?: Array<tg.Process.Child> | undefined;
		command: tg.Command;
		createdAt: number;
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
			return {
				cached: value.cached,
				options,
				process: value.process.id,
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
					id: data.process,
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
				}),
			};
		};
	}

	export namespace State {
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
			if (value.error !== undefined) {
				output.error = tg.Error.toData(value.error);
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
				output.log = value.log.id;
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
				error:
					data.error !== undefined
						? typeof data.error === "string"
							? tg.Error.withId(data.error)
							: tg.Error.fromData(data.error)
						: undefined,
				exit: data.exit,
				expectedChecksum: data.expected_checksum,
				finishedAt: data.finished_at,
				host: data.host,
				log: data.log !== undefined ? tg.Blob.withId(data.log) : undefined,
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

	export type Stdio = "inherit" | "log" | "null" | "pipe" | "tty";

	export namespace Stdio {
		export type Chunk = {
			bytes: Uint8Array;
			position?: number | undefined;
			stream: tg.Process.Stdio.Stream;
		};

		export type Stream = "stdin" | "stdout" | "stderr";

		export namespace Read {
			export type Arg = {
				length?: number | undefined;
				location?: tg.Location.Arg | undefined;
				position?: number | string | undefined;
				size?: number | undefined;
				streams: Array<tg.Process.Stdio.Stream>;
			};

			export type Event =
				| { kind: "chunk"; value: tg.Process.Stdio.Chunk }
				| { kind: "end" };

			export namespace Event {
				export type Data =
					| { kind: "chunk"; value: tg.Process.Stdio.Read.Event.Data.Chunk }
					| { kind: "end" };

				export namespace Data {
					export type Chunk = {
						bytes: string;
						position?: number | undefined;
						stream: tg.Process.Stdio.Stream;
					};
				}

				export let fromData = (
					data: tg.Process.Stdio.Read.Event.Data,
				): tg.Process.Stdio.Read.Event => {
					if (data.kind === "chunk") {
						return {
							kind: "chunk",
							value: {
								bytes: tg.encoding.base64.decode(data.value.bytes),
								position: data.value.position,
								stream: data.value.stream,
							},
						};
					} else {
						return data;
					}
				};

				export let toData = (
					event: tg.Process.Stdio.Read.Event,
				): tg.Process.Stdio.Read.Event.Data => {
					if (event.kind === "chunk") {
						return {
							kind: "chunk",
							value: {
								bytes: tg.encoding.base64.encode(event.value.bytes),
								position: event.value.position,
								stream: event.value.stream,
							},
						};
					} else {
						return event;
					}
				};
			}
		}

		export namespace Write {
			export type Arg = {
				location?: tg.Location.Arg | undefined;
				streams: Array<tg.Process.Stdio.Stream>;
			};

			export type Event = { kind: "end" } | { kind: "stop" };
		}

		export class Reader {
			#available: boolean;
			#fd: number | undefined;
			#input: AsyncIterableIterator<tg.Process.Stdio.Read.Event> | undefined;
			#process: tg.Process | undefined;
			#stream: "stdout" | "stderr";

			constructor(arg: {
				fd?: number | undefined;
				unavailable?: boolean | undefined;
				stream: "stdout" | "stderr";
			}) {
				this.#available = !(arg.unavailable ?? false);
				this.#fd = arg.fd;
				this.#input = undefined;
				this.#process = undefined;
				this.#stream = arg.stream;
			}

			setProcess(process: tg.Process): void {
				this.#process = process;
			}

			async close(): Promise<void> {
				let fd = this.#fd;
				let input = this.#input;
				this.#fd = undefined;
				this.#input = undefined;
				this.#process = undefined;
				if (!this.#available) {
					return;
				}
				if (fd !== undefined) {
					await tg.host.close(fd);
				}
				if (input !== undefined) {
					await input.return?.();
				}
			}

			async read(): Promise<Uint8Array | undefined> {
				if (!this.#available) {
					throw new Error(`${this.#stream} is not available`);
				}
				if (this.#fd !== undefined) {
					while (true) {
						let bytes = await tg.host.read(this.#fd, 4096);
						if (bytes === undefined) {
							break;
						}
						if (bytes.length > 0) {
							return bytes;
						}
					}
					let fd = this.#fd;
					this.#fd = undefined;
					this.#process = undefined;
					if (fd !== undefined) {
						await tg.host.close(fd);
					}
					return undefined;
				}
				if (this.#process === undefined) {
					throw new Error(`${this.#stream} is not available`);
				}
				if (this.#input === undefined) {
					let input = await tg.handle.readProcessStdio(this.#process.id, {
						location: this.#process.location,
						streams: [this.#stream],
					});
					if (input === undefined) {
						throw new Error(`${this.#stream} is not available`);
					}
					this.#input = input;
				}
				while (true) {
					let result = await this.#input.next();
					if (result.done) {
						break;
					}
					let event = result.value;
					if (event.kind === "end") {
						break;
					}
					if (event.value.stream !== this.#stream) {
						throw new Error("invalid process stdio stream");
					}
					if (event.value.bytes.length > 0) {
						return event.value.bytes;
					}
				}
				this.#input = undefined;
				this.#process = undefined;
				return undefined;
			}

			async readAll(): Promise<Uint8Array> {
				let chunks: Array<Uint8Array> = [];
				let length = 0;
				while (true) {
					let bytes = await this.read();
					if (bytes === undefined) {
						break;
					}
					chunks.push(bytes);
					length += bytes.length;
				}
				let output = new Uint8Array(length);
				let position = 0;
				for (let chunk of chunks) {
					output.set(chunk, position);
					position += chunk.length;
				}
				return output;
			}

			async text(): Promise<string> {
				return tg.encoding.utf8.decode(await this.readAll());
			}
		}

		export class Writer {
			#available: boolean;
			#fd: number | undefined;
			#process: tg.Process | undefined;
			#stream: "stdin";

			constructor(arg: {
				fd?: number | undefined;
				unavailable?: boolean | undefined;
				stream: "stdin";
			}) {
				this.#available = !(arg.unavailable ?? false);
				this.#fd = arg.fd;
				this.#process = undefined;
				this.#stream = arg.stream;
			}

			setProcess(process: tg.Process): void {
				this.#process = process;
			}

			async close(): Promise<void> {
				let fd = this.#fd;
				let process = this.#process;
				let stream = this.#stream;
				if (!this.#available) {
					this.#fd = undefined;
					this.#process = undefined;
					return;
				}
				if (fd !== undefined) {
					this.#fd = undefined;
					this.#process = undefined;
					await tg.host.close(fd);
					return;
				}
				if (process !== undefined) {
					if (process.pid !== undefined) {
						this.#fd = undefined;
						this.#process = undefined;
						return;
					}
					let location = process.location;
					if (location === undefined) {
						await process.load();
						location = process.location;
					}
					this.#fd = undefined;
					this.#process = undefined;
					await tg.handle.writeProcessStdio(
						process.id,
						{
							location,
							streams: [stream],
						},
						(async function* () {
							yield { kind: "end" };
						})(),
					);
				}
			}

			async write(input: Uint8Array): Promise<number> {
				if (!(input instanceof Uint8Array)) {
					throw new Error("expected stdio bytes");
				}
				if (!this.#available) {
					throw new Error(`${this.#stream} is not available`);
				}
				let fd = this.#fd;
				let process = this.#process;
				let stream = this.#stream;
				if (fd === undefined && process === undefined) {
					throw new Error(`${stream} is not available`);
				}
				if (input.length === 0) {
					return 0;
				}
				if (fd !== undefined) {
					await tg.host.write(fd, input);
					return input.length;
				}
				if (process!.pid !== undefined) {
					throw new Error(`${stream} is not available`);
				}
				let location = process!.location;
				if (location === undefined) {
					await process!.load();
					location = process!.location;
				}
				await tg.handle.writeProcessStdio(
					process!.id,
					{
						location,
						streams: [stream],
					},
					(async function* () {
						yield {
							kind: "chunk",
							value: {
								bytes: input,
								stream,
							},
						};
					})(),
				);
				return input.length;
			}

			async writeAll(input: Uint8Array): Promise<void> {
				if (!(input instanceof Uint8Array)) {
					throw new Error("expected stdio bytes");
				}
				let position = 0;
				while (position < input.length) {
					let count = await this.write(input.subarray(position));
					if (count === 0) {
						throw new Error("failed to write stdin");
					}
					position += count;
				}
				await this.close();
			}
		}
	}

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

	export type Status = "created" | "started" | "finished";

	export type Data = {
		actual_checksum?: tg.Checksum;
		cacheable?: boolean;
		children?: Array<tg.Process.Data.Child>;
		command: tg.Command.Id;
		created_at: number;
		error?: tg.Error.Data | tg.Error.Id;
		exit?: number;
		expected_checksum?: tg.Checksum;
		finished_at?: number;
		host: string;
		log?: tg.Blob.Id;
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
			process: tg.Process.Id;
		};
	}

	export type Wait = {
		error: tg.Error | undefined;
		exit: number;
		output?: tg.Value;
	};

	export namespace Wait {
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

async function stdioTask(
	id: tg.Process.Id,
	location: tg.Location.Arg | undefined,
	stdin: "pipe" | "tty" | undefined,
	stdout: "pipe" | "tty" | undefined,
	stderr: "pipe" | "tty" | undefined,
	tty: boolean,
): Promise<void> {
	let stdinError: unknown;
	let stdinClosing = false;
	let stdinStopper =
		stdin !== undefined ? await tg.host.stopperOpen() : undefined;
	let stdinTask_ =
		stdin !== undefined && stdinStopper !== undefined
			? stdinTask(id, location, stdin, stdinStopper).catch((error) => {
					if (!stdinClosing) {
						stdinError = error;
					}
				})
			: undefined;
	let sigwinchError: unknown;
	let sigwinchListener = tty ? tg.host.listenSignal("sigwinch") : undefined;
	let sigwinchTask_ =
		sigwinchListener !== undefined
			? sigwinchTask(id, location, sigwinchListener).catch((error) => {
					sigwinchError = error;
				})
			: undefined;
	let stdoutStderrError: unknown;
	try {
		stdoutStderrError = await stdoutStderrTask(
			id,
			location,
			stdout,
			stderr,
		).then(
			() => undefined,
			(error) => error,
		);
		if (stdinStopper !== undefined) {
			stdinClosing = true;
			await tg.host.stopperStop(stdinStopper);
		}
	} finally {
		await cleanupStdio(stdinStopper, sigwinchListener);
	}
	if (stdinTask_ !== undefined) {
		await stdinTask_;
	}
	if (sigwinchTask_ !== undefined) {
		await sigwinchTask_;
	}
	if (stdinError !== undefined) {
		throw stdinError;
	}
	if (sigwinchError !== undefined) {
		throw sigwinchError;
	}
	if (stdoutStderrError !== undefined) {
		throw stdoutStderrError;
	}
}

async function cleanupStdio(
	stdinStopper: tg.Host.Stopper | undefined,
	sigwinchListener: tg.Host.SignalListener | undefined,
): Promise<void> {
	try {
		if (sigwinchListener !== undefined) {
			await sigwinchListener.close();
		}
	} finally {
		if (stdinStopper !== undefined) {
			await tg.host.stopperClose(stdinStopper);
		}
	}
}

async function stdinTask(
	id: tg.Process.Id,
	location: tg.Location.Arg | undefined,
	stdin: "pipe" | "tty",
	stopper: tg.Host.Stopper,
): Promise<void> {
	let error: unknown;
	let raw = stdin === "tty" && tg.host.isForegroundControllingTty(0);
	if (raw) {
		await tg.host.enableRawMode(0);
	}
	try {
		let input =
			(async function* (): AsyncIterableIterator<tg.Process.Stdio.Read.Event> {
				while (true) {
					let bytes = await tg.host.read(0, 4096, stopper);
					if (bytes === undefined) {
						break;
					}
					if (bytes.length === 0) {
						continue;
					}
					yield {
						kind: "chunk",
						value: {
							bytes,
							stream: "stdin",
						},
					};
				}
				yield { kind: "end" };
			})();
		await tg.handle.writeProcessStdio(
			id,
			{
				location,
				streams: ["stdin"],
			},
			input,
		);
	} catch (error_) {
		error = error_;
	} finally {
		if (raw) {
			try {
				await tg.host.disableRawMode(0);
			} catch (disableError) {
				if (error === undefined) {
					error = disableError;
				}
			}
		}
	}
	if (error !== undefined) {
		throw error;
	}
}

async function stdoutStderrTask(
	id: tg.Process.Id,
	location: tg.Location.Arg | undefined,
	stdout: "pipe" | "tty" | undefined,
	stderr: "pipe" | "tty" | undefined,
): Promise<void> {
	let streams: Array<tg.Process.Stdio.Stream> = [];
	if (stdout !== undefined) {
		streams.push("stdout");
	}
	if (stderr !== undefined) {
		streams.push("stderr");
	}
	if (streams.length === 0) {
		return;
	}
	let iterator = await tg.handle.readProcessStdio(id, {
		location,
		streams,
	});
	if (iterator === undefined) {
		return;
	}
	for await (let event of iterator) {
		if (event.kind === "end") {
			break;
		}
		let fd = event.value.stream === "stdout" ? 1 : 2;
		await tg.host.write(fd, event.value.bytes);
	}
}

async function sigwinchTask(
	id: tg.Process.Id,
	location: tg.Location.Arg | undefined,
	signalListener: tg.Host.SignalListener,
): Promise<void> {
	for await (let _ of signalListener) {
		let size = tg.host.getTtySize();
		if (size === undefined) {
			continue;
		}
		await tg.handle.setProcessTtySize(id, {
			location,
			size,
		});
	}
}

async function checkoutArtifacts(
	command: tg.Command.Object,
): Promise<Map<tg.Artifact.Id, string>> {
	let artifacts = new Set<tg.Artifact.Id>();
	let data = tg.Command.Object.toData(command);
	for (let object of tg.Command.Data.children(data)) {
		if (tg.Artifact.Id.is(object)) {
			artifacts.add(object);
		}
	}
	let output = new Map<tg.Artifact.Id, string>();
	for (let artifact of artifacts) {
		let path = await tg.handle.checkout({
			artifact,
			dependencies: true,
			force: false,
			lock: undefined,
			path: undefined,
		});
		output.set(artifact, path);
	}
	return output;
}

function renderCommand(
	command: tg.Command.Object,
	artifacts: Map<tg.Artifact.Id, string>,
	outputPath: string,
): { args: Array<string>; executable: string } {
	switch (command.host) {
		case "builtin": {
			let args = renderArgsDashA(command.args);
			args.unshift("builtin", renderExecutableUri(command.executable));
			return { args, executable: "tangram" };
		}
		case "js": {
			let args = renderArgsDashA(command.args);
			args.unshift("js", renderExecutableUri(command.executable));
			return { args, executable: "tangram" };
		}
		default: {
			return {
				args: renderArgsString(command.args, artifacts, outputPath),
				executable: renderExecutable(command.executable, artifacts),
			};
		}
	}
}

function renderExecutable(
	executable: tg.Command.Executable,
	artifacts: Map<tg.Artifact.Id, string>,
): string {
	if ("artifact" in executable) {
		let path = artifacts.get(executable.artifact.id);
		if (path === undefined) {
			throw new Error("failed to find the executable artifact path");
		}
		return tg.path.join(path, executable.path);
	} else if ("module" in executable) {
		throw new Error("invalid executable");
	} else {
		return executable.path;
	}
}

function renderExecutableUri(executable: tg.Command.Executable): string {
	if ("artifact" in executable) {
		let string = executable.artifact.id;
		if (executable.path !== undefined) {
			string += `?path=${encodeURIComponent(executable.path)}`;
		}
		return string;
	} else if ("module" in executable) {
		let string = tg.Module.toDataString(executable.module);
		if (executable.export !== undefined) {
			string += `#${encodeURIComponent(executable.export)}`;
		}
		return string;
	} else {
		return executable.path;
	}
}

function renderArgsDashA(args: Array<tg.Value>): Array<string> {
	return args.flatMap((value) => ["-A", tg.Value.stringify(value)]);
}

function renderArgsString(
	args: Array<tg.Value>,
	artifacts: Map<tg.Artifact.Id, string>,
	outputPath: string,
): Array<string> {
	return args.map((value) => renderValueString(value, artifacts, outputPath));
}

async function renderEnv(
	env: { [key: string]: tg.Value },
	artifacts: Map<tg.Artifact.Id, string>,
	outputPath: string,
): Promise<{ [key: string]: string }> {
	for (let key of Object.keys(env)) {
		if (key.startsWith("TANGRAM_ENV_")) {
			throw new Error("env vars prefixed with TANGRAM_ENV_ are reserved");
		}
	}
	let resolved: { [key: string]: tg.Value } = {};
	for (let [key, value] of Object.entries(env)) {
		if (value instanceof tg.Mutation) {
			await value.apply(resolved, key);
		} else {
			resolved[key] = value;
		}
	}
	let rendered: { [key: string]: string } = {};
	for (let [key, value] of Object.entries(resolved)) {
		rendered[key] = renderValueString(value, artifacts, outputPath);
	}
	for (let [key, value] of Object.entries(resolved)) {
		if (typeof value === "string") {
			continue;
		}
		rendered[`TANGRAM_ENV_${key}`] = tg.Value.stringify(value);
	}
	rendered.TANGRAM_OUTPUT = outputPath;
	return rendered;
}

function renderValueString(
	value: tg.Value,
	artifacts: Map<tg.Artifact.Id, string>,
	outputPath: string,
): string {
	if (typeof value === "string") {
		return value;
	}
	if (tg.Artifact.is(value)) {
		let path = artifacts.get(value.id);
		if (path === undefined) {
			throw new Error("failed to find the artifact path");
		}
		return path;
	}
	if (value instanceof tg.Template) {
		return value.components
			.map((component) => {
				if (typeof component === "string") {
					return component;
				}
				if (tg.Artifact.is(component)) {
					let path = artifacts.get(component.id);
					if (path === undefined) {
						throw new Error("failed to find the artifact path");
					}
					return path;
				}
				if (component.name === "output") {
					return outputPath;
				}
				throw new Error("invalid placeholder");
			})
			.join("");
	}
	if (value instanceof tg.Placeholder) {
		if (value.name === "output") {
			return outputPath;
		}
		throw new Error("invalid placeholder");
	}
	return tg.Value.stringify(value);
}

function renderStdio(
	stdio: string,
	stream: "stdin" | "stdout" | "stderr",
): "inherit" | "null" | "pipe" {
	switch (stdio) {
		case "inherit":
		case "null":
		case "pipe": {
			return stdio;
		}
		case "log": {
			throw new Error("log stdio is not supported for unsandboxed processes");
		}
		case "tty": {
			throw new Error("tty stdio is not supported for unsandboxed processes");
		}
		default: {
			if (stream === "stdin") {
				throw new Error(
					"blob stdin is not supported for unsandboxed processes",
				);
			}
			throw new Error("blob stdio is not supported for unsandboxed processes");
		}
	}
}

let isSandboxArg = (value: unknown): value is tg.Sandbox.Arg => {
	return typeof value === "object" && value !== null && !Array.isArray(value);
};

let normalizeSandbox = (
	arg: Pick<
		tg.Process.ArgObject,
		"cpu" | "memory" | "mounts" | "network" | "sandbox"
	>,
): Exclude<tg.Handle.SpawnArg["sandbox"], undefined> | undefined => {
	let hasCpu = "cpu" in arg;
	let cpu = arg.cpu;
	let hasMemory = "memory" in arg;
	let memory = arg.memory;
	let mounts = arg.mounts ?? [];
	let hasNetwork = "network" in arg;
	let network = arg.network ?? false;
	let sandbox = arg.sandbox;
	if (typeof sandbox === "string") {
		if (hasCpu || hasMemory || mounts.length > 0 || hasNetwork) {
			throw new Error(
				"cpu, memory, mounts, and network are not supported for existing sandboxes",
			);
		}
		return sandbox;
	}
	if (sandbox === undefined || sandbox === false) {
		if (!hasCpu && !hasMemory && mounts.length === 0 && !hasNetwork) {
			return undefined;
		}
		sandbox = { network: false };
	}
	if (sandbox === true) {
		sandbox = { network: false };
	}
	let output: tg.Handle.SandboxArg = { network: false };
	if (isSandboxArg(sandbox)) {
		if (sandbox.cpu !== undefined) {
			output.cpu = sandbox.cpu;
		}
		if (sandbox.hostname !== undefined) {
			output.hostname = sandbox.hostname;
		}
		if (sandbox.memory !== undefined) {
			output.memory = sandbox.memory;
		}
		if (sandbox.mounts !== undefined) {
			output.mounts = sandbox.mounts.map(tg.Sandbox.Mount.toDataString);
		}
		output.network = sandbox.network ?? false;
		if (sandbox.ttl !== undefined) {
			output.ttl = sandbox.ttl;
		}
		if (sandbox.user !== undefined) {
			output.user = sandbox.user;
		}
	}
	if (hasCpu) {
		output.cpu = cpu;
	}
	if (hasMemory) {
		output.memory = memory;
	}
	if (mounts.length > 0) {
		output.mounts = [
			...(output.mounts ?? []),
			...mounts.map(tg.Sandbox.Mount.toDataString),
		];
	}
	if (hasNetwork) {
		output.network = network;
	}
	return output;
};
