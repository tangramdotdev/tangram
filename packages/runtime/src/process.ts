import * as tg from "./index.ts";
import { flatten } from "./util.ts";

export let process: tg.Process;

export let setProcess = async (process_: Process) => {
	process = process_;
};

export class Process {
	#id: tg.Process.Id;
	#remote: string | undefined;
	#state: tg.Process.State | undefined;

	constructor(arg: tg.Process.ConstructorArg) {
		this.#id = arg.id;
		this.#remote = arg.remote;
		this.#state = arg.state;
	}

	static async spawn(
		...args: tg.Args<tg.Process.SpawnArg>
	): Promise<tg.Process> {
		let arg = await Process.arg(...args);
		let checksum = arg.checksum;
		let command = await tg.command(
			{ env: tg.process.command().then((command) => command.env()) },
			arg.command,
			{
				host: arg.host,
				executable: arg.executable,
				env: arg.env,
				args: arg.args,
			},
		);
		let cwd = "cwd" in arg ? arg.cwd : await tg.process.cwd();
		let network = "network" in arg ? arg.network : await tg.process.network();
		let id = await syscall("process_spawn", {
			checksum,
			command: await command.id(),
			create: false,
			cwd,
			env: arg.processEnv,
			network,
			parent: undefined,
			remote: undefined,
			retry: false,
		});
		return new tg.Process({
			id,
			remote: undefined,
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
				env: undefined,
				processEnv: {},
				network: false,
			},
			...args,
		);
		let output = await process.output();
		return output;
	}

	static async run(...args: tg.Args<tg.Process.SpawnArg>): Promise<tg.Value> {
		let process = await Process.spawn(...args);
		let output = await process.output();
		return output;
	}

	async output(): Promise<tg.Value> {
		let output = await this.wait();
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
						host: (await process.env())!.TANGRAM_HOST,
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

	async env(): Promise<{ [name: string]: string } | undefined> {
		await this.load();
		return this.#state!.env;
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
		network?: boolean | undefined;
	};

	export type State = {
		checksum: tg.Checksum | undefined;
		command: tg.Command;
		cwd: string | undefined;
		env: { [key: string]: string } | undefined;
		error: tg.Error | undefined;
		exit: tg.Process.Exit | undefined;
		network: boolean;
		output: tg.Value | undefined;
		status: tg.Process.Status;
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
