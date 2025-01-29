import * as tg from "./index.ts";
import { flatten } from "./util.ts";

export let process: {
	checksum: tg.Checksum | undefined;
	command: {
		args: Array<tg.Value>;
		env: { [key: string]: tg.Value };
		executable: tg.Command.Executable | undefined;
		host: string;
		stdin: tg.Blob.Id | undefined;
	};
	cwd: string | undefined;
	env: { [key: string]: string };
	network: boolean | undefined;
};

export let setProcess = async (process_: Process) => {
	let checksum = await process_.checksum();
	let command_ = await process_.command();
	let command = {
		args: await command_.args(),
		env: await command_.env(),
		executable: await command_.executable(),
		host: await command_.host(),
		stdin: undefined,
	};
	let cwd = await process_.cwd();
	let network = await process_.network();
	let env = await process_.env();
	process = {
		env,
		checksum,
		command,
		cwd,
		network,
	};
};

export class Process {
	#id: tg.Process.Id;
	#state: tg.Process.State | undefined;

	constructor(arg: tg.Process.ConstructorArg) {
		this.#id = arg.id;
		this.#state = arg.state;
	}

	static async spawn(
		...args: tg.Args<tg.Process.SpawnArg>
	): Promise<tg.Process> {
		let arg = await Process.arg(...args);
		let id = await syscall("process_spawn", arg);
		return new tg.Process({ id, state: undefined });
	}

	async wait(): Promise<tg.Value> {
		let output = await syscall("process_wait", this.#id);
		return output;
	}

	static async build(...args: tg.Args<tg.Process.SpawnArg>): Promise<tg.Value> {
		let arg = await Process.arg(...args);
		let checksum = arg.checksum;
		let command = await tg.command(arg.command, {
			host: arg.host,
			executable: arg.executable,
			env: arg.env,
			args: arg.args,
		});
		let cwd = arg.cwd ?? undefined;
		let network = arg.network ?? false;
		let process = await Process.spawn({
			checksum,
			command,
			cwd,
			network,
		});
		let output = await process.wait();
		return output;
	}

	static async run(...args: tg.Args<tg.Process.SpawnArg>): Promise<tg.Value> {
		let arg = await Process.arg(...args);
		let checksum = arg.checksum;
		let cwd = arg.cwd ?? tg.process.cwd;
		let command = await tg.command({ env: tg.process.env }, arg.command, {
			host: arg.host,
			executable: arg.executable,
			env: arg.env,
			args: arg.args,
		});
		let network = arg.network ?? tg.process.network;
		let process = await Process.spawn({
			checksum,
			command,
			cwd,
			network,
		});
		let output = await process.wait();
		return output;
	}

	static withId(id: tg.Process.Id): tg.Process {
		return new Process({ id, state: undefined });
	}

	static expect(value: unknown): tg.Process {
		tg.assert(value instanceof Process);
		return value;
	}

	static assert(value: unknown): asserts value is tg.Process {
		tg.assert(value instanceof Process);
	}

	async load(): Promise<void> {
		let state = await syscall("process_load", this.#id);
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
						host: process.env.TANGRAM_HOST,
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

	async env(): Promise<{ [name: string]: string }> {
		await this.load();
		return this.#state!.env;
	}

	async network(): Promise<boolean | undefined> {
		await this.load();
		return this.#state!.network;
	}
}

export namespace Process {
	export type ConstructorArg = {
		id: tg.Process.Id;
		state: State | undefined;
	};

	export type Id = string;

	export type SpawnArg =
		| undefined
		| string
		| tg.Artifact
		| tg.Template
		| tg.Command
		| SpawnArgObject;

	export type SpawnArgObject = {
		/** The command's command line arguments. */
		args?: Array<tg.Value> | undefined;

		/** If a checksum of the process's output is provided, then the process can be cached even if it is not sandboxed. */
		checksum?: tg.Checksum | undefined;

		/** The command to spawn. **/
		command?: tg.Command.Arg | undefined;

		/** Set the current working directory for the process. **/
		cwd?: string | undefined;

		/** The process's command's environment variables. */
		env?: tg.MaybeNestedArray<tg.MaybeMutationMap> | undefined;

		/** The command's executable. */
		executable?: tg.Command.ExecutableArg | undefined;

		/** The system to build the command on. */
		host?: string | undefined;

		/** Configure whether the process has network access. **/
		network?: boolean | undefined;
	};

	export type State = {
		checksum: tg.Checksum | undefined;
		command: tg.Command;
		cwd: string | undefined;
		env: { [key: string]: string };
		network: boolean | undefined;
	};
}
