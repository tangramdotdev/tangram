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

export class Process {
	#id: tg.Process.Id;
	#remote: string | undefined;
	#token: string | undefined;
	#state: tg.Process.State | undefined;
	#stdioPromise: Promise<void> | undefined;
	#unsandboxed: tg.Process.Unsandboxed | undefined;
	#wait: tg.Process.Wait | undefined;

	constructor(arg: tg.Process.ConstructorArg) {
		this.#id = arg.id;
		this.#remote = arg.remote;
		this.#state = arg.state;
		this.#stdioPromise = arg.stdioPromise;
		this.#token = arg.token;
		this.#unsandboxed = arg.unsandboxed;
		this.#wait = arg.wait;
	}

	get state(): tg.Process.State | undefined {
		return this.#state;
	}

	static async spawn(arg: tg.Handle.SpawnArg): Promise<tg.Process> {
		if (!arg.sandbox) {
			return await spawnUnsandboxedProcess(arg);
		}
		return await spawnSandboxedProcess(arg);
	}

	async signal(signal: tg.Process.Signal): Promise<void> {
		if (this.#unsandboxed !== undefined) {
			await tg.host.signal(this.#unsandboxed.pid, signal);
			return;
		}
		let arg = {
			local: this.#remote === undefined ? true : undefined,
			remotes: this.#remote !== undefined ? [this.#remote] : undefined,
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
		if (this.#unsandboxed !== undefined) {
			let wait = await this.#unsandboxed.promise;
			this.#wait = wait;
			return wait;
		}
		let remotes = undefined;
		if (this.#remote) {
			remotes = [this.#remote];
		}
		let arg = {
			local: undefined,
			remotes,
			token: this.#token,
		};
		let data = await tg.handle.waitProcess(this.#id, arg);
		let wait = tg.Process.Wait.fromData(data);
		this.#wait = wait;
		return wait;
	}

	async readStdio(
		arg: tg.Process.Stdio.Read.Arg,
	): Promise<AsyncIterableIterator<tg.Process.Stdio.Read.Event> | undefined> {
		if (arg.streams.length === 0) {
			throw new Error("expected at least one stdio stream");
		}
		if (this.#unsandboxed !== undefined) {
			return readUnsandboxedProcessStdio(this.#unsandboxed, arg);
		}
		return await tg.handle.readProcessStdio(
			this.#id,
			normalizeProcessStdioReadArg(this.#remote, arg),
		);
	}

	async setTtySize(size: tg.Process.Tty.Size): Promise<void> {
		if (this.#unsandboxed !== undefined) {
			throw new Error(
				"tty resizing is not supported for unsandboxed processes",
			);
		}
		await tg.handle.setProcessTtySize(this.#id, {
			local: undefined,
			remotes: this.#remote !== undefined ? [this.#remote] : undefined,
			size,
		});
	}

	static expect(value: unknown): tg.Process {
		tg.assert(value instanceof Process);
		return value;
	}

	static assert(value: unknown): asserts value is tg.Process {
		tg.assert(value instanceof Process);
	}

	async load(): Promise<void> {
		if (this.#unsandboxed !== undefined) {
			throw new Error("loading unsandboxed process state is not supported");
		}
		let data = await tg.handle.getProcess(this.#id, this.#remote);
		this.#state = tg.Process.State.fromData(data);
	}

	async reload(): Promise<void> {
		await this.load();
	}

	get id(): tg.Process.Id {
		return this.#id;
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

	async writeStdio(
		arg: tg.Process.Stdio.Write.Arg,
		input: AsyncIterableIterator<tg.Process.Stdio.Read.Event>,
	): Promise<void> {
		if (this.#unsandboxed !== undefined) {
			await writeUnsandboxedProcessStdio(this.#unsandboxed, arg, input);
			return;
		}
		await tg.handle.writeProcessStdio(
			this.#id,
			normalizeProcessStdioWriteArg(this.#remote, arg),
			input,
		);
	}

	get mounts(): Promise<Array<tg.Process.Mount>> {
		return (async () => {
			await this.load();
			return this.#state!.mounts;
		})();
	}

	get network(): Promise<boolean> {
		return (async () => {
			await this.load();
			return this.#state!.network;
		})();
	}

	get user(): Promise<string | undefined> {
		return (async () => {
			return await (
				await this.command
			).user;
		})();
	}
}

export namespace Process {
	export type Id = string;

	export type ConstructorArg = {
		id: tg.Process.Id;
		remote?: string | undefined;
		state?: State | undefined;
		stdioPromise?: Promise<void> | undefined;
		token?: string | undefined;
		unsandboxed?: tg.Process.Unsandboxed | undefined;
		wait?: tg.Process.Wait | undefined;
	};

	export type Unsandboxed = {
		pid: number;
		promise: Promise<tg.Process.Wait>;
		stdin?: UnsandboxedStdin | undefined;
		stdout?: UnsandboxedStdoutOrStderr | undefined;
		stderr?: UnsandboxedStdoutOrStderr | undefined;
	};

	export type RunArg =
		| undefined
		| string
		| tg.Artifact
		| tg.Template
		| tg.Command
		| RunArgObject;

	export type RunArgObject = {
		args?: Array<tg.Value> | undefined;
		checksum?: tg.Checksum | undefined;
		cwd?: string | undefined;
		env?: tg.MaybeMutationMap | undefined;
		executable?: tg.Command.Arg.Executable | undefined;
		host?: string | undefined;
		mounts?: Array<tg.Process.Mount> | undefined;
		name?: string | undefined;
		network?: boolean | undefined;
		sandbox?: boolean | undefined;
		stderr?: tg.Process.Stdio.Value | undefined;
		stdin?: tg.Blob.Arg | tg.Process.Stdio.Value | undefined;
		stdout?: tg.Process.Stdio.Value | undefined;
		tty?: boolean | tg.Process.Tty | undefined;
		user?: string | undefined;
	};

	export type State = {
		command: tg.Command;
		error: tg.Error | undefined;
		exit: number | undefined;
		mounts: Array<tg.Process.Mount>;
		network: boolean;
		output?: tg.Value;
		status: tg.Process.Status;
		stderr: string | undefined;
		stdin: string | undefined;
		stdout: string | undefined;
	};

	export namespace State {
		export let toData = (value: State): Data => {
			let output: Data = {
				command: value.command.id,
				status: value.status,
			};
			if (value.error !== undefined) {
				output.error = tg.Error.toData(value.error);
			}
			if (value.exit !== undefined) {
				output.exit = value.exit;
			}
			if (value.mounts.length > 0) {
				output.mounts = value.mounts;
			}
			if (value.network) {
				output.network = value.network;
			}
			if ("output" in value) {
				output.output = tg.Value.toData(value.output);
			}
			if (value.stderr !== undefined) {
				output.stderr = value.stderr;
			}
			if (value.stdin !== undefined) {
				output.stdin = value.stdin;
			}
			if (value.stdout !== undefined) {
				output.stdout = value.stdout;
			}
			return output;
		};

		export let fromData = (data: tg.Process.Data): tg.Process.State => {
			let output: State = {
				command: tg.Command.withId(data.command),
				error:
					data.error !== undefined
						? typeof data.error === "string"
							? tg.Error.withId(data.error)
							: tg.Error.fromData(data.error)
						: undefined,
				exit: data.exit,
				mounts: data.mounts ?? [],
				network: data.network ?? false,
				status: data.status,
				stderr: data.stderr,
				stdin: data.stdin,
				stdout: data.stdout,
			};
			if ("output" in data) {
				output.output = tg.Value.fromData(data.output);
			}
			return output;
		};
	}

	export type Mount = {
		source: string;
		target: string;
		readonly?: boolean;
	};

	export type Tty = {
		size: tg.Process.Tty.Size;
	};

	export namespace Tty {
		export type Size = {
			cols: number;
			rows: number;
		};
	}

	export namespace Stdio {
		export type Value = "inherit" | "log" | "null" | "pipe" | "tty";

		export type Chunk = {
			bytes: Uint8Array;
			position?: number | undefined;
			stream: tg.Process.Stdio.Stream;
		};

		export type Stream = "stdin" | "stdout" | "stderr";

		export namespace Read {
			export type Arg = {
				length?: number | undefined;
				local?: boolean | undefined;
				position?: number | string | undefined;
				remotes?: Array<string> | undefined;
				size?: number | undefined;
				streams: Array<tg.Process.Stdio.Stream>;
			};

			export type Event =
				| { kind: "chunk"; value: tg.Process.Stdio.Chunk }
				| { kind: "end" };
		}

		export namespace Write {
			export type Arg = {
				local?: boolean | undefined;
				remotes?: Array<string> | undefined;
				streams: Array<tg.Process.Stdio.Stream>;
			};

			export type Event = { kind: "end" } | { kind: "stop" };
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
		command: tg.Command.Id;
		error?: tg.Error.Data | tg.Error.Id;
		exit?: number;
		mounts?: Array<tg.Process.Mount>;
		network?: boolean;
		output?: tg.Value.Data;
		status: tg.Process.Status;
		stderr?: string;
		stdin?: string;
		stdout?: string;
	};

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

async function spawnSandboxedProcess(
	arg: tg.Handle.SpawnArg,
): Promise<tg.Process> {
	let noTty = arg.tty === false;
	let tty: tg.Process.Tty | undefined;
	if (arg.tty === true) {
		let size = tg.host.getTtySize();
		if (size !== undefined) {
			tty = { size };
		}
	} else if (arg.tty !== undefined && arg.tty !== false) {
		tty = arg.tty;
	}
	let stdin: "pipe" | "tty" | undefined =
		arg.stdin === "inherit"
			? !noTty && tg.host.isTty(0)
				? "tty"
				: "pipe"
			: undefined;
	let stdout: "pipe" | "tty" | undefined =
		arg.stdout === "inherit"
			? !noTty && tg.host.isTty(1)
				? "tty"
				: "pipe"
			: undefined;
	let stderr: "pipe" | "tty" | undefined =
		arg.stderr === "inherit"
			? !noTty && tg.host.isTty(2)
				? "tty"
				: "pipe"
			: undefined;
	if (
		tty === undefined &&
		(stdin === "tty" || stdout === "tty" || stderr === "tty")
	) {
		let size = tg.host.getTtySize();
		if (size !== undefined) {
			tty = { size };
		}
	}
	let output = await tg.handle.spawnProcess({
		...arg,
		stderr: stderr ?? arg.stderr,
		stdin: stdin ?? arg.stdin,
		stdout: stdout ?? arg.stdout,
		tty,
	});
	let wait =
		output.wait !== undefined
			? tg.Process.Wait.fromData(output.wait)
			: undefined;
	let stdioPromise =
		stdin !== undefined ||
		stdout !== undefined ||
		stderr !== undefined ||
		tty !== undefined
			? runStdioTask(
					output.process,
					output.remote,
					stdin,
					stdout,
					stderr,
					tty !== undefined,
				)
			: undefined;
	let process = new tg.Process({
		id: output.process,
		remote: output.remote,
		state: undefined,
		stdioPromise,
		token: output.token,
		wait,
	});
	return process;
}

async function runStdioTask(
	id: tg.Process.Id,
	remote: string | undefined,
	stdin: "pipe" | "tty" | undefined,
	stdout: "pipe" | "tty" | undefined,
	stderr: "pipe" | "tty" | undefined,
	tty: boolean,
): Promise<void> {
	let stdinError: unknown;
	let stdinClosing = false;
	let stdinListener = stdin !== undefined ? tg.host.stdin(4096) : undefined;
	let stdinTask_ =
		stdinListener !== undefined
			? stdinTask(id, remote, stdinListener).catch((error) => {
					if (!stdinClosing) {
						stdinError = error;
					}
				})
			: undefined;
	let sigwinchError: unknown;
	let signalListener = tty ? tg.host.listenSignal("sigwinch") : undefined;
	let sigwinchTask_ =
		signalListener !== undefined
			? sigwinchTask(id, remote, signalListener).catch((error) => {
					sigwinchError = error;
				})
			: undefined;
	let outputError: unknown;
	try {
		await outputTask(id, remote, stdout, stderr);
	} catch (error) {
		outputError = error;
	}
	if (stdinListener !== undefined) {
		stdinClosing = true;
		await stdinListener.close();
	}
	if (signalListener !== undefined) {
		await signalListener.close();
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
	if (outputError !== undefined) {
		throw outputError;
	}
}

function createProcessStdioReadArg(
	remote: string | undefined,
	streams: Array<tg.Process.Stdio.Stream>,
): tg.Process.Stdio.Read.Arg {
	return normalizeProcessStdioReadArg(remote, { streams });
}

function createProcessStdioWriteArg(
	remote: string | undefined,
	streams: Array<tg.Process.Stdio.Stream>,
): tg.Process.Stdio.Write.Arg {
	return normalizeProcessStdioWriteArg(remote, { streams });
}

function normalizeProcessStdioReadArg(
	remote: string | undefined,
	arg: tg.Process.Stdio.Read.Arg,
): tg.Process.Stdio.Read.Arg {
	return {
		...arg,
		local: arg.local,
		remotes: arg.remotes ?? (remote !== undefined ? [remote] : undefined),
	};
}

function normalizeProcessStdioWriteArg(
	remote: string | undefined,
	arg: tg.Process.Stdio.Write.Arg,
): tg.Process.Stdio.Write.Arg {
	return {
		...arg,
		local: arg.local,
		remotes: arg.remotes ?? (remote !== undefined ? [remote] : undefined),
	};
}

function readUnsandboxedProcessStdio(
	unsandboxed: tg.Process.Unsandboxed,
	arg: tg.Process.Stdio.Read.Arg,
): AsyncIterableIterator<tg.Process.Stdio.Read.Event> | undefined {
	if (
		arg.position !== undefined ||
		arg.length !== undefined ||
		arg.size !== undefined
	) {
		throw new Error(
			"position, length, and size are only valid for logged stdio",
		);
	}
	let iterators: Array<AsyncIterableIterator<tg.Process.Stdio.Read.Event>> = [];
	for (let stream of arg.streams) {
		switch (stream) {
			case "stdin":
				throw new Error("reading stdin is invalid");
			case "stdout": {
				let stdout = unsandboxed.stdout?.read();
				if (stdout !== undefined) {
					iterators.push(stdout);
				}
				break;
			}
			case "stderr": {
				let stderr = unsandboxed.stderr?.read();
				if (stderr !== undefined) {
					iterators.push(stderr);
				}
				break;
			}
		}
	}
	if (iterators.length === 0) {
		return undefined;
	}
	if (iterators.length === 1) {
		let iterator = iterators[0]!;
		return (async function* (): AsyncIterableIterator<tg.Process.Stdio.Read.Event> {
			for await (let event of iterator) {
				yield event;
			}
			yield { kind: "end" };
		})();
	}
	return (async function* (): AsyncIterableIterator<tg.Process.Stdio.Read.Event> {
		let states = iterators.map((iterator) => ({
			iterator,
			next: iterator.next(),
		}));
		try {
			while (states.length > 0) {
				let { result, state } = await Promise.race(
					states.map(async (state) => ({
						result: await state.next,
						state,
					})),
				);
				if (result.done) {
					states = states.filter((other) => other !== state);
					continue;
				}
				state.next = state.iterator.next();
				yield result.value;
			}
		} finally {
			await Promise.all(
				states
					.map((state) => state.iterator.return?.())
					.filter((result) => result !== undefined),
			);
		}
		yield { kind: "end" };
	})();
}

async function writeUnsandboxedProcessStdio(
	unsandboxed: tg.Process.Unsandboxed,
	arg: tg.Process.Stdio.Write.Arg,
	input: AsyncIterableIterator<tg.Process.Stdio.Read.Event>,
): Promise<void> {
	if (arg.streams.length !== 1 || arg.streams[0] !== "stdin") {
		throw new Error("writing stdout or stderr is invalid");
	}
	let stdin = unsandboxed.stdin;
	if (stdin === undefined) {
		throw new Error("stdin is not available");
	}
	await stdin.write(input);
}

async function stdinTask(
	id: tg.Process.Id,
	remote: string | undefined,
	stdinListener: tg.Host.StdinListener,
): Promise<void> {
	let input =
		(async function* (): AsyncIterableIterator<tg.Process.Stdio.Read.Event> {
			for await (let bytes of stdinListener) {
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
		createProcessStdioWriteArg(remote, ["stdin"]),
		input,
	);
}

async function outputTask(
	id: tg.Process.Id,
	remote: string | undefined,
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
	let iterator = await tg.handle.readProcessStdio(
		id,
		createProcessStdioReadArg(remote, streams),
	);
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
	remote: string | undefined,
	signalListener: tg.Host.SignalListener,
): Promise<void> {
	for await (let _ of signalListener) {
		let size = tg.host.getTtySize();
		if (size === undefined) {
			throw new Error("failed to get the tty size");
		}
		await tg.handle.setProcessTtySize(id, {
			local: undefined,
			remotes: remote !== undefined ? [remote] : undefined,
			size,
		});
	}
}

async function spawnUnsandboxedProcess(
	arg: tg.Handle.SpawnArg,
): Promise<tg.Process> {
	if (arg.tty !== undefined) {
		throw new Error("tty is not supported for unsandboxed processes");
	}
	if (arg.mounts.length > 0) {
		throw new Error("mounts are not supported for unsandboxed processes");
	}
	if (arg.stdin.startsWith("blb_")) {
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
	let tempDir = await tg.host.mkdtemp();
	let outputPath = tg.path.join(tempDir, id);
	let artifacts = await checkoutArtifacts(command);
	let env = await renderEnv(command.env, artifacts, outputPath);
	let { args, executable } = renderCommand(command, artifacts, outputPath);
	let spawnOutput = await tg.host.spawn({
		args,
		cwd: command.cwd,
		env,
		executable,
		stderr: renderStdio(arg.stderr, "stderr"),
		stdin: renderStdio(arg.stdin, "stdin"),
		stdout: renderStdio(arg.stdout, "stdout"),
	});
	let unsandboxed: tg.Process.Unsandboxed = {
		pid: spawnOutput.pid,
		promise: undefined as any,
		stdin:
			spawnOutput.stdin !== undefined
				? new UnsandboxedStdin(spawnOutput.stdin)
				: undefined,
		stdout:
			spawnOutput.stdout !== undefined
				? new UnsandboxedStdoutOrStderr(spawnOutput.stdout, "stdout")
				: undefined,
		stderr:
			spawnOutput.stderr !== undefined
				? new UnsandboxedStdoutOrStderr(spawnOutput.stderr, "stderr")
				: undefined,
	};
	unsandboxed.promise = waitForUnsandboxedProcess(
		unsandboxed,
		tempDir,
		outputPath,
	);
	return new tg.Process({
		id,
		remote: undefined,
		state: undefined,
		token: undefined,
		unsandboxed,
	});
}

async function waitForUnsandboxedProcess(
	unsandboxed: tg.Process.Unsandboxed,
	tempDir: string,
	outputPath: string,
): Promise<tg.Process.Wait> {
	let wait: tg.Process.Wait | undefined;
	let waitError: unknown;
	try {
		let output = await tg.host.wait(unsandboxed.pid);
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
			let errorBytes = await tg.host.getxattr(outputPath, "user.tangram.error");
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
			let stdio = unsandboxed[name];
			unsandboxed[name] = undefined;
			if (stdio !== undefined) {
				await stdio.close();
			}
		}
		await tg.host.remove(tempDir);
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

async function checkoutArtifacts(
	command: tg.Command.Object,
): Promise<Map<tg.Artifact.Id, string>> {
	let artifacts = new Set<tg.Artifact.Id>();
	let seen = new Set<tg.Object.Id>();
	let stack = tg.Command.Object.children(command);
	while (stack.length > 0) {
		let object = stack.pop()!;
		let id = object.id;
		if (seen.has(id)) {
			continue;
		}
		seen.add(id);
		if (tg.Artifact.is(object)) {
			artifacts.add(object.id);
		}
		let children = await object.children;
		stack.push(...children);
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
	let output: { [key: string]: tg.Value } = {};
	for (let [key, value] of Object.entries(env)) {
		if (value instanceof tg.Mutation) {
			await value.apply(output, key);
		} else {
			output[key] = value;
		}
	}
	let rendered: { [key: string]: string } = {};
	for (let [key, value] of Object.entries(output)) {
		rendered[key] = renderValueString(value, artifacts, outputPath);
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

class UnsandboxedStdin {
	#fd: number | undefined;

	constructor(fd: number) {
		this.#fd = fd;
	}

	async close(): Promise<void> {
		let fd = this.#take();
		if (fd !== undefined) {
			await tg.host.close(fd);
		}
	}

	async write(
		input: AsyncIterableIterator<tg.Process.Stdio.Read.Event>,
	): Promise<void> {
		let fd = this.#take();
		if (fd === undefined) {
			throw new Error("stdin is not available");
		}
		try {
			for await (let event of input) {
				if (event.kind === "end") {
					break;
				}
				if (event.value.stream !== "stdin") {
					throw new Error("invalid process stdio stream");
				}
				await tg.host.write(fd, event.value.bytes);
			}
		} finally {
			await tg.host.close(fd);
		}
	}

	#take(): number | undefined {
		let fd = this.#fd;
		this.#fd = undefined;
		return fd;
	}
}

class UnsandboxedStdoutOrStderr {
	#fd: number | undefined;
	#stream: "stdout" | "stderr";

	constructor(fd: number, stream: "stdout" | "stderr") {
		this.#fd = fd;
		this.#stream = stream;
	}

	async close(): Promise<void> {
		let fd = this.#take();
		if (fd !== undefined) {
			await tg.host.close(fd);
		}
	}

	read(): AsyncIterableIterator<tg.Process.Stdio.Read.Event> | undefined {
		let fd = this.#take();
		if (fd === undefined) {
			return undefined;
		}
		let stream = this.#stream;
		return (async function* (): AsyncIterableIterator<tg.Process.Stdio.Read.Event> {
			try {
				while (true) {
					let bytes = await tg.host.read(fd, 4096);
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
							stream,
						},
					};
				}
			} finally {
				await tg.host.close(fd);
			}
		})();
	}

	#take(): number | undefined {
		let fd = this.#fd;
		this.#fd = undefined;
		return fd;
	}
}
