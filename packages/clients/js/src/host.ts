import type * as tg from "./index.ts";

export let host: Host = {} as any;

export let setHost = (newHost: Host) => {
	Object.assign(host, newHost);
};

export type Host = {
	close(fd: number): Promise<void>;

	exists(path: string): Promise<boolean>;

	getTtySize(): tg.Process.Tty.Size | undefined;

	getxattr(path: string, name: string): Promise<Uint8Array | undefined>;

	isTty(fd: number): boolean;

	listenSignal(signal: tg.Host.Signal): tg.Host.SignalListener;

	magic(value: Function): tg.Command.Data.Executable;

	mkdtemp(): Promise<string>;

	read(
		fd: number,
		length?: number | undefined,
	): Promise<Uint8Array | undefined>;

	remove(path: string): Promise<void>;

	signal(pid: number, signal: tg.Process.Signal): Promise<void>;

	sleep(duration: number): Promise<void>;

	spawn(arg: tg.Host.SpawnArg): Promise<tg.Host.SpawnOutput>;

	stdin(length?: number | undefined): tg.Host.StdinListener;

	wait(pid: number): Promise<tg.Host.WaitOutput>;

	write(fd: number, bytes: Uint8Array): Promise<void>;

	writeSync(fd: number, bytes: Uint8Array): void;
};

export namespace Host {
	export type Signal = "sigwinch";

	export type SignalListener = AsyncIterable<void> & {
		close(): Promise<void>;
	};

	export type StdinListener = AsyncIterable<Uint8Array> & {
		close(): Promise<void>;
	};

	export type SpawnArg = {
		executable: string;
		args: Array<string>;
		cwd?: string | undefined;
		env: { [key: string]: string };
		stdin: "inherit" | "null" | "pipe";
		stdout: "inherit" | "null" | "pipe";
		stderr: "inherit" | "null" | "pipe";
	};

	export type SpawnOutput = {
		pid: number;
		stdin?: number | undefined;
		stdout?: number | undefined;
		stderr?: number | undefined;
	};

	export type WaitOutput = {
		exit: number;
	};
}
