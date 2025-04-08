import * as tg from "./index.ts";
import { flatten, mergeMaybeMutationMaps } from "./util.ts";

export class Process {
	static current: tg.Process;

	#id: tg.Process.Id;
	#remote: string | undefined;
	#state: tg.Process.State | undefined;

	constructor(arg: tg.Process.ConstructorArg) {
		this.#id = arg.id;
		this.#remote = arg.remote;
		this.#state = arg.state;
	}

	get state(): tg.Process.State | undefined {
		return this.#state;
	}

	static async build(...args: tg.Args<tg.Process.BuildArg>): Promise<tg.Value> {
		let arg = await Process.buildArg(...args);
		let commandMounts: Array<tg.Command.Mount> | undefined;
		if ("mounts" in arg && arg.mounts !== undefined) {
			commandMounts = await Promise.all(
				arg.mounts.map(async (mount) => {
					if (typeof mount === "string" || mount instanceof tg.Template) {
						return await tg.Command.Mount.parse(mount);
					} else {
						return mount;
					}
				}),
			);
		}
		let commandStdin: tg.Blob.Arg | undefined = undefined;
		if ("stdin" in arg && arg.stdin !== undefined) {
			commandStdin = arg.stdin;
		}
		let command = await tg.command(
			"args" in arg ? { args: arg.args } : undefined,
			"cwd" in arg ? { cwd: arg.cwd } : undefined,
			"env" in arg ? { env: arg.env } : undefined,
			"executable" in arg ? { executable: arg.executable } : undefined,
			"host" in arg ? { host: arg.host } : undefined,
			commandMounts !== undefined ? { mounts: commandMounts } : undefined,
			commandStdin !== undefined ? { stdin: commandStdin } : undefined,
		);
		let checksum = arg.checksum;
		let network = "network" in arg ? arg.network : false;
		if (network === true && checksum === undefined) {
			throw new Error("a checksum is required to build with network enabled");
		}
		let commandId = await command.id();
		let spawnOutput = await syscall("process_spawn", {
			checksum,
			command: commandId,
			create: false,
			mounts: [],
			network,
			parent: undefined,
			remote: undefined,
			retry: false,
			stderr: undefined,
			stdin: undefined,
			stdout: undefined,
		});
		let process = new tg.Process({
			id: spawnOutput.process,
			remote: spawnOutput.remote,
			state: undefined,
		});
		let wait = await process.wait();
		if (wait.error) {
			throw wait.error;
		}
		if (
			wait.exit !== undefined &&
			"code" in wait.exit &&
			wait.exit.code !== 0
		) {
			throw new Error(`the process exited with code ${wait.exit.code}`);
		}
		if (wait.exit !== undefined && "signal" in wait.exit) {
			throw new Error(`the process exited with signal ${wait.exit.signal}`);
		}
		return wait.output;
	}

	static async buildArg(
		...args: tg.Args<tg.Process.BuildArg>
	): Promise<tg.Process.BuildArgObject> {
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
					};
				} else if (arg instanceof tg.Command) {
					return { command: await arg.object() };
				} else {
					return arg;
				}
			}),
		);
		let mutations = await tg.Args.createMutations(objects, {
			args: "append",
			env: "append",
		});
		let arg = await mergeMaybeMutationMaps(mutations);
		return arg;
	}

	static async run(...args: tg.Args<tg.Process.RunArg>): Promise<tg.Value> {
		let state = tg.Process.current.#state!;
		let currentCommand = await Process.current.command();
		let arg = await Process.runArg(...args);
		let checksum = arg.checksum;
		let processMounts: Array<tg.Process.Mount> = [];
		let commandMounts: Array<tg.Command.Mount> | undefined;
		if ("mounts" in arg && arg.mounts !== undefined) {
			for (let mount of arg.mounts) {
				if (typeof mount === "string" || mount instanceof tg.Template) {
					try {
						let commandMount = await tg.Command.Mount.parse(mount);
						if (commandMounts === undefined) {
							commandMounts = [];
						}
						commandMounts.push(commandMount);
					} catch {}
					try {
						let processMount = await tg.Process.Mount.parse(mount);
						processMounts.push(processMount);
					} catch {}
				} else {
					if (tg.Artifact.is(mount.source)) {
						if (commandMounts === undefined) {
							commandMounts = [];
						}
						commandMounts.push(mount as tg.Command.Mount);
					} else {
						processMounts.push(mount as tg.Process.Mount);
					}
				}
			}
		} else {
			commandMounts = await currentCommand.mounts();
			processMounts = state.mounts;
		}
		let processStdin = state.stdin;
		let commandStdin: tg.Blob.Arg | undefined = undefined;
		if ("stdin" in arg) {
			processStdin = undefined;
			if (arg.stdin !== undefined) {
				commandStdin = arg.stdin;
			}
		} else {
			commandStdin = await currentCommand.stdin();
		}
		let stderr = state.stdout;
		if ("stderr" in arg) {
			stderr = arg.stderr;
		}
		let stdout = state.stdout;
		if ("stdout" in arg) {
			stdout = arg.stdout;
		}
		let command = await tg.command(
			{
				cwd: currentCommand.cwd(),
				env: currentCommand.env(),
			},
			"args" in arg ? { args: arg.args } : undefined,
			"cwd" in arg ? { cwd: arg.cwd } : undefined,
			"env" in arg ? { env: arg.env } : undefined,
			"executable" in arg ? { executable: arg.executable } : undefined,
			"host" in arg ? { host: arg.host } : undefined,
			commandMounts !== undefined ? { mounts: commandMounts } : undefined,
			commandStdin !== undefined ? { stdin: commandStdin } : undefined,
		);
		let network = "network" in arg ? arg.network : state.network;
		let commandId = await command.id();
		let output = await syscall("process_spawn", {
			checksum,
			command: commandId,
			create: false,
			mounts: processMounts,
			network,
			parent: undefined,
			remote: undefined,
			retry: false,
			stderr,
			stdin: processStdin,
			stdout,
		});
		let process = new tg.Process({
			id: output.process,
			remote: output.remote,
			state: undefined,
		});
		let wait = await process.wait();
		if (wait.error) {
			throw wait.error;
		}
		if (
			wait.exit !== undefined &&
			"code" in wait.exit &&
			wait.exit.code !== 0
		) {
			throw new Error(`the process exited with code ${wait.exit.code}`);
		}
		if (wait.exit !== undefined && "signal" in wait.exit) {
			throw new Error(`the process exited with signal ${wait.exit.signal}`);
		}
		return wait.output;
	}

	static async runArg(
		...args: tg.Args<tg.Process.RunArg>
	): Promise<tg.Process.RunArgObject> {
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
					};
				} else if (arg instanceof tg.Command) {
					return { command: await arg.object() };
				} else {
					return arg;
				}
			}),
		);
		let mutations = await tg.Args.createMutations(objects, {
			args: "append",
			env: "append",
		});
		let arg = await mergeMaybeMutationMaps(mutations);
		return arg;
	}

	async wait(): Promise<tg.Process.WaitOutput> {
		let output = await syscall("process_wait", this.#id, this.#remote);
		return output;
	}

	static expect(value: unknown): tg.Process {
		tg.assert(value instanceof Process);
		return value;
	}

	static assert(value: unknown): asserts value is tg.Process {
		tg.assert(value instanceof Process);
	}

	async load(): Promise<void> {
		let state = await syscall("process_load", this.#id, this.#remote);
		this.#state = state;
	}

	async reload(): Promise<void> {
		await this.load();
	}

	id(): tg.Process.Id {
		return this.#id;
	}

	async checksum(): Promise<tg.Checksum | undefined> {
		await this.load();
		return this.#state!.checksum;
	}

	async command(): Promise<tg.Command> {
		await this.load();
		return this.#state!.command;
	}

	async args(): Promise<Array<tg.Value>> {
		return await (await this.command()).args();
	}

	async cwd(): Promise<string | undefined> {
		return await (await this.command()).cwd();
	}

	async env(): Promise<{ [key: string]: tg.Value }>;
	async env(name: string): Promise<tg.Value | undefined>;
	async env(
		name?: string,
	): Promise<{ [name: string]: tg.Value } | tg.Value | undefined> {
		let commandEnv = await (await this.command()).env();
		if (name === undefined) {
			return commandEnv;
		} else {
			return commandEnv[name];
		}
	}

	async mounts(): Promise<Array<tg.Command.Mount | tg.Process.Mount>> {
		let commandMounts = await (await this.command()).mounts();
		await this.load();
		return [...this.#state!.mounts, ...commandMounts];
	}

	async network(): Promise<boolean> {
		await this.load();
		return this.#state!.network;
	}

	async user(): Promise<string | undefined> {
		return await (await this.command()).user();
	}
}

export namespace Process {
	export type ConstructorArg = {
		id: tg.Process.Id;
		remote: string | undefined;
		state: State | undefined;
	};

	export type Exit = { code: number } | { signal: number };

	export type Id = string;

	export type Mount = {
		source: string;
		target: string;
		readonly: boolean;
	};

	export namespace Mount {
		export let parse = async (
			arg: string | tg.Template,
		): Promise<tg.Process.Mount> => {
			// If the user passed a template, ensure it contains no artifacts.
			let s: string | undefined;
			if (typeof arg === "string") {
				s = arg;
			} else if (arg instanceof tg.Template) {
				s = await arg.components.reduce(async (acc, component) => {
					if (tg.Artifact.is(component)) {
						throw new Error("expected no artifacts");
					} else {
						return (await acc) + component;
					}
				}, Promise.resolve(""));
			} else {
				throw new Error("expected a template or a string");
			}
			tg.assert(s);
			let readonly: boolean | undefined = undefined;

			// Handle readonly/readwrite option if present
			if (s.includes(",")) {
				const [mountPart, option] = s.split(",", 2);
				tg.assert(mountPart);
				tg.assert(option);

				if (option === "ro") {
					readonly = true;
					s = mountPart;
				} else if (option === "rw") {
					readonly = false;
					s = mountPart;
				} else {
					throw new Error(`unknown option: "${option}"`);
				}
			}

			// Split into source and target
			const colonIndex = s.indexOf(":");
			if (colonIndex === -1) {
				throw new Error("expected a target path");
			}

			const sourcePart = s.substring(0, colonIndex);
			const targetPart = s.substring(colonIndex + 1);

			// Validate target is absolute path
			if (!targetPart.startsWith("/")) {
				throw new Error(`expected an absolute path: "${targetPart}"`);
			}

			// Determine source type
			let source = sourcePart;

			readonly = readonly ?? false;

			return {
				source,
				target: targetPart,
				readonly,
			};
		};
	}

	export type BuildArg =
		| undefined
		| string
		| tg.Artifact
		| tg.Template
		| tg.Command
		| BuildArgObject;

	export type BuildArgObject = {
		args?: Array<tg.Value> | undefined;
		checksum?: tg.Checksum | undefined;
		cwd?: string | undefined;
		env?: tg.MaybeNestedArray<tg.MaybeMutationMap> | undefined;
		executable?: tg.Command.ExecutableArg | undefined;
		host?: string | undefined;
		mounts?: Array<string | tg.Template | tg.Command.Mount> | undefined;
		network?: boolean | undefined;
		stdin?: tg.Blob.Arg | undefined;
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
		env?: tg.MaybeNestedArray<tg.MaybeMutationMap> | undefined;
		executable?: tg.Command.ExecutableArg | undefined;
		host?: string | undefined;
		mounts?:
			| Array<string | tg.Template | tg.Command.Mount | tg.Process.Mount>
			| undefined;
		network?: boolean | undefined;
		stderr?: undefined;
		stdin?: tg.Blob.Arg | undefined;
		stdout?: undefined;
		user?: string | undefined;
	};

	export type State = {
		checksum: tg.Checksum | undefined;
		command: tg.Command;
		error: tg.Error | undefined;
		exit: tg.Process.Exit | undefined;
		mounts: Array<tg.Process.Mount>;
		network: boolean;
		output: tg.Value | undefined;
		status: tg.Process.Status;
		stderr?: string;
		stdin?: string;
		stdout?: string;
	};

	export type Status =
		| "created"
		| "enqueued"
		| "dequeued"
		| "started"
		| "finishing"
		| "finished";

	export type WaitOutput = {
		error: tg.Error | undefined;
		exit: tg.Process.Exit | undefined;
		output: tg.Value | undefined;
		status: tg.Process.Status;
	};
}
