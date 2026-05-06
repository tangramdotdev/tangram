import * as tg from "../index.ts";
import * as stdio from "./stdio.ts";

export let builder = (...args: any): any => {
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
};

export let spawnArg = async (
	...args: tg.Args<tg.Process.Arg>
): Promise<{
	arg: tg.Handle.SpawnArg;
	options: tg.Referent.Options;
}> => {
	let arg = await tg.Process.arg(...args);

	let sandbox = normalizeSandbox(arg);

	if (sandbox === undefined) {
		let cwd = tg.process.cwd;
		let env = { ...tg.process.env };
		arg = await tg.Process.arg({ cwd, env }, arg);
	} else {
		if (!("host" in arg)) {
			arg.host = defaultHost();
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
		debug:
			arg.debug === undefined || arg.debug === false
				? undefined
				: arg.debug === true
					? {}
					: arg.debug,
		location: arg.location,
		retry: false,
		sandbox,
		stderr: stderr ?? "inherit",
		stdin: processStdin ?? "inherit",
		stdout: stdout ?? "inherit",
		tty,
	};

	return { arg: spawnArg, options };
};

export let spawnUnsandboxed = async <O extends tg.Value = tg.Value>(
	arg: tg.Handle.SpawnArg,
	options?: tg.Referent.Options,
): Promise<tg.Process<O>> => {
	let prepared = await prepareUnsandboxedCommand(arg);
	let spawnOutput = await tg.host.spawn({
		args: prepared.args,
		cwd: prepared.cwd,
		env: prepared.env,
		executable: prepared.executable,
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
	let id = spawnOutput.pid;
	let promise = waitUnsandboxed(
		id,
		{
			stderr,
			stdin,
			stdout,
		},
		prepared.tempPath,
		prepared.outputPath,
	);
	return new tg.Process<O>({
		id,
		location: undefined,
		options: options ?? {},
		promise,
		state: undefined,
		stderr,
		stdin,
		token: undefined,
		stdout,
	});
};

export let waitUnsandboxed = async (
	pid: number,
	stdio: {
		stderr: tg.Process.Stdio.Reader;
		stdin: tg.Process.Stdio.Writer;
		stdout: tg.Process.Stdio.Reader;
	},
	tempPath: string,
	outputPath: string,
): Promise<tg.Process.Wait> => {
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
};

export let prepareUnsandboxedCommand = async (
	arg: tg.Handle.SpawnArg,
	outputPath?: string,
): Promise<tg.Process.PreparedUnsandboxedCommandOutput> => {
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

	let tempPath = await tg.host.mkdtemp();
	outputPath ??= tg.path.join(tempPath, "output");
	let artifacts = await checkoutArtifacts(command);
	let env = await renderEnv(command.env, artifacts, outputPath);
	let { args, executable } = renderCommand(
		command,
		artifacts,
		outputPath,
		arg.debug,
	);
	return {
		args,
		cwd: command.cwd,
		env,
		executable,
		tempPath,
		outputPath,
	};
};

export let spawnSandboxed = async <O extends tg.Value = tg.Value>(
	arg: tg.Handle.SpawnArg,
	options?: tg.Referent.Options,
): Promise<tg.Process<O>> => {
	let noTty = arg.tty === false;
	let provideStderr = arg.stderr === "pipe" || arg.stderr === "tty";
	let provideStdin = arg.stdin === "pipe" || arg.stdin === "tty";
	let provideStdout = arg.stdout === "pipe" || arg.stdout === "tty";
	let stdinIsTty = tg.host.isTty(0);
	let stdinIsForegroundControllingTty = tg.host.isForegroundControllingTty(0);
	let stdoutIsForegroundControllingTty = tg.host.isForegroundControllingTty(1);
	let stderrIsForegroundControllingTty = tg.host.isForegroundControllingTty(2);
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
		let spawn: tg.Process.Stdio = !noTty && foregroundTty ? "tty" : background;
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
		(spawnStdin === "tty" || spawnStdout === "tty" || spawnStderr === "tty") &&
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
			? stdio.task(output.process, location, stdin, stdout, stderr, localTty)
			: undefined;
	let process = new tg.Process<O>({
		id: output.process,
		location,
		options: options ?? {},
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
};

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
	debug: tg.Process.Debug | undefined,
): { args: Array<string>; executable: string } {
	switch (command.host) {
		case "builtin": {
			let args = renderArgsDashA(command.args);
			args.unshift("builtin", renderExecutableUri(command.executable));
			return { args, executable: "tangram" };
		}
		case "js": {
			let args = [
				"js",
				...renderJsDebugArgs(debug),
				renderExecutableUri(command.executable),
				...renderArgsDashA(command.args),
			];
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

function renderJsDebugArgs(debug: tg.Process.Debug | undefined): Array<string> {
	if (debug === undefined) {
		return [];
	}
	let args = ["--debug"];
	if (debug.addr !== undefined) {
		args.push("--debug-addr", debug.addr);
	}
	if (debug.mode !== undefined && debug.mode !== "normal") {
		args.push("--debug-mode", debug.mode);
	}
	return args;
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
	for (let key of [
		"TANGRAM_CONFIG",
		"TANGRAM_DIRECTORY",
		"TANGRAM_MODE",
		"TANGRAM_OUTPUT",
		"TANGRAM_PROCESS",
		"TANGRAM_TOKEN",
		"TANGRAM_TRACING",
		"TANGRAM_URL",
	]) {
		delete rendered[key];
	}
	let arg = tg.handle.arg();
	rendered.TANGRAM_OUTPUT = outputPath;
	if (arg.process !== undefined) {
		rendered.TANGRAM_PROCESS = arg.process;
	}
	if (arg.token !== undefined) {
		rendered.TANGRAM_TOKEN = arg.token;
	}
	if (arg.url !== undefined) {
		rendered.TANGRAM_URL = arg.url;
	}
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

export let isSandboxArg = (value: unknown): value is tg.Sandbox.Arg => {
	return typeof value === "object" && value !== null && !Array.isArray(value);
};

export let isNetworkEnabled = (
	value: boolean | tg.Sandbox.Network | undefined,
): boolean => {
	if (value) {
		if (typeof value === "boolean") {
			return value;
		}
		return true;
	} else {
		return false;
	}
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
	let defaultTtl = typeof sandbox !== "string";
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
		if (sandbox.isolation !== undefined) {
			output.isolation = tg.Sandbox.Isolation.toData(sandbox.isolation);
		}
		if (sandbox.location !== undefined) {
			output.location = sandbox.location;
		}
		if (sandbox.memory !== undefined) {
			output.memory = sandbox.memory;
		}
		if (sandbox.mounts !== undefined) {
			output.mounts = sandbox.mounts.map(tg.Sandbox.Mount.toDataString);
		}
		output.network = normalizeNetwork(sandbox.network);
		if ("ttl" in sandbox) {
			output.ttl = sandbox.ttl;
		} else if (defaultTtl) {
			output.ttl = 0;
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
		output.network = normalizeNetwork(network);
	}
	return output;
};

let normalizeNetwork = (
	value: boolean | tg.Sandbox.Network | undefined,
): boolean | tg.Sandbox.Network.Data => {
	if (value === undefined) {
		return false;
	}
	if (typeof value === "boolean") {
		return value;
	}
	return tg.Sandbox.Network.toData(value);
};

export let defaultHost = (): string | undefined => {
	return (tg.process.env.TANGRAM_HOST as string | undefined) ?? tg.host.current;
};
