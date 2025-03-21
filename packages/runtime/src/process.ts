import { mutate } from "./args.ts";
import * as tg from "./index.ts";
import { flatten } from "./util.ts";

export class Process {
	static current: tg.Process;

	#env: { [name: string]: tg.Value } | undefined;
	#id: tg.Process.Id;
	#remote: string | undefined;
	#state: tg.Process.State | undefined;

	constructor(arg: tg.Process.ConstructorArg) {
		this.#env = undefined;
		this.#id = arg.id;
		this.#remote = arg.remote;
		this.#state = arg.state;
	}

	get state(): tg.Process.State | undefined {
		return this.#state;
	}

	static async spawn(
		...args: tg.Args<tg.Process.SpawnArg>
	): Promise<tg.Process> {
		let { mounts, ...arg } = await Process.arg(...args);
		let checksum = arg.checksum;
		let command = await tg.command(
			{ env: Process.current.command().then((command) => command.env()) },
			arg.command,
			arg,
		);
		let cwd = "cwd" in arg ? arg.cwd : tg.Process.current.#state!.cwd;
		let processEnv =
			"processEnv" in arg ? arg.processEnv : tg.Process.current.#state!.env;
		let mounts_ = await Promise.all(
			(mounts ?? []).map(async (mount) => {
				if (typeof mount === "string" || mount instanceof tg.Template) {
					return await tg.Process.Mount.parse(mount);
				} else {
					return mount;
				}
			}),
		);
		let network =
			"network" in arg ? arg.network : tg.Process.current.#state!.network;
		let output = await syscall("process_spawn", {
			checksum,
			command: await command.id(),
			create: false,
			cwd,
			env: processEnv,
			mounts: mounts_,
			network,
			parent: undefined,
			remote: undefined,
			retry: false,
		});
		return new tg.Process({
			id: output.process,
			remote: output.remote,
			state: undefined,
		});
	}

	async wait(): Promise<tg.Process.WaitOutput> {
		let output = await syscall("process_wait", this.#id, this.#remote);
		return output;
	}

	static async build(...args: tg.Args<tg.Process.SpawnArg>): Promise<tg.Value> {
		let process = await Process.spawn(
			{
				cwd: undefined,
				processEnv: undefined,
				network: false,
			},
			...args,
		);
		let output = await process.wait();
		if (output.status !== "succeeded") {
			throw output.error;
		}
		return output.output;
	}

	static async run(...args: tg.Args<tg.Process.SpawnArg>): Promise<tg.Value> {
		let process = await Process.spawn(...args);
		let output = await process.wait();
		if (output.status !== "succeeded") {
			throw output.error;
		}
		return output.output;
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

	static async arg(
		...args: tg.Args<tg.Process.SpawnArg>
	): Promise<tg.Process.SpawnArgObject> {
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
						host: (await (await tg.Process.current.command()).env())!
							.TANGRAM_HOST,
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
		let arg = await tg.Args.applyMutations(mutations);
		return arg;
	}

	async checksum(): Promise<tg.Checksum | undefined> {
		await this.load();
		return this.#state!.checksum;
	}

	async command(): Promise<tg.Command> {
		await this.load();
		return this.#state!.command;
	}

	async cwd(): Promise<string | undefined> {
		await this.load();
		return this.#state!.cwd;
	}

	async env(): Promise<{ [name: string]: tg.Value }>;
	async env(name: string): Promise<tg.Value | undefined>;
	async env(
		name?: string,
	): Promise<{ [name: string]: tg.Value } | tg.Value | undefined> {
		if (this.#env === undefined) {
			await this.load();
			let processEnv = this.#state!.env;
			let commandEnv = await (await this.command()).env();
			this.#env = processEnv ?? {};
			for (let [name, value] of Object.entries(commandEnv)) {
				mutate(this.#env, name, value);
			}
		}
		if (name === undefined) {
			return this.#env!;
		} else {
			return this.#env![name];
		}
	}

	async network(): Promise<boolean> {
		await this.load();
		return this.#state!.network;
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
		source: tg.Process.Mount.Source;
		target: string;
		readonly: boolean;
	};

	export namespace Mount {
		export type Source = tg.Artifact | string;

		export let parse = async (
			t: string | tg.Template,
		): Promise<tg.Process.Mount> => {
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
			let source: Mount.Source = sourcePart;

			readonly = readonly ?? false;

			return {
				source,
				target: targetPart,
				readonly,
			};
		};
	}

	export type SpawnArg =
		| undefined
		| string
		| tg.Artifact
		| tg.Template
		| tg.Command
		| SpawnArgObject;

	export type SpawnArgObject = {
		args?: Array<tg.Value> | undefined;
		checksum?: tg.Checksum | undefined;
		command?: tg.Command.Arg | undefined;
		cwd?: string | undefined;
		processEnv?: { [key: string]: string } | undefined;
		env?: tg.MaybeNestedArray<tg.MaybeMutationMap> | undefined;
		executable?: tg.Command.ExecutableArg | undefined;
		host?: string | undefined;
		mounts?: Array<string | tg.Template | tg.Process.Mount> | undefined;
		network?: boolean | undefined;
	};

	export type State = {
		checksum: tg.Checksum | undefined;
		command: tg.Command;
		cwd: string | undefined;
		env: { [key: string]: string } | undefined;
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
		| "canceled"
		| "failed"
		| "succeeded";

	export type WaitOutput = {
		error: tg.Error | undefined;
		exit: tg.Process.Exit | undefined;
		output: tg.Value | undefined;
		status: tg.Process.Status;
	};
}
