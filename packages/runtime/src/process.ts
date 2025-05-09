import * as tg from "./index.ts";

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

	async executable(): Promise<tg.Command.Executable> {
		return await (await this.command()).executable();
	}

	async mounts(): Promise<Array<tg.Command.Mount | tg.Process.Mount>> {
		let commandMounts = await (await this.command()).mounts();
		await this.load();
		return [...this.#state!.mounts, ...(commandMounts ?? [])];
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
		env?: tg.MaybeMutationMap | undefined;
		executable?: tg.Command.ExecutableArg | undefined;
		host?: string | undefined;
		mounts?: Array<string | tg.Template | tg.Command.Mount> | undefined;
		network?: boolean | undefined;
		stdin?: tg.Blob.Arg | undefined;
		user?: string | undefined;
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
		command: tg.Command;
		error: tg.Error | undefined;
		exit: number | undefined;
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
		exit: number;
		output: tg.Value | undefined;
		status: tg.Process.Status;
	};
}
