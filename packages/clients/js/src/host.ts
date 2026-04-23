import type * as tg from "./index.ts";

export let host: Host = {} as any;

export let setHost = (newHost: Host) => {
	Object.assign(host, newHost);
};

export type Host = {
	close(fd: number): Promise<void>;

	disableRawMode(fd: number): Promise<void>;

	enableRawMode(fd: number): Promise<void>;

	exec(arg: tg.Host.SpawnArg): Promise<never>;

	exists(path: string): Promise<boolean>;

	getTtySize(): tg.Process.Tty.Size | undefined;

	getxattr(path: string, name: string): Promise<Uint8Array | undefined>;

	isForegroundControllingTty(fd: number): boolean;

	isTty(fd: number): boolean;

	listenSignal(signal: tg.Host.Signal): tg.Host.SignalListener;

	magic(value: Function): tg.Command.Data.Executable;

	mkdtemp(): Promise<string>;

	read(
		fd: number,
		length?: number | undefined,
		stopper?: tg.Host.Stopper | undefined,
	): Promise<Uint8Array | undefined>;

	remove(path: string): Promise<void>;

	signal(pid: number, signal: tg.Process.Signal): Promise<void>;

	sleep(duration: number, stopper?: tg.Host.Stopper | undefined): Promise<void>;

	spawn(arg: tg.Host.SpawnArg): Promise<tg.Host.SpawnOutput>;

	stopperClose(stopper: tg.Host.Stopper): Promise<void>;

	stopperOpen(): Promise<tg.Host.Stopper>;

	stopperStop(stopper: tg.Host.Stopper): Promise<void>;

	wait(
		pid: number,
		stopper?: tg.Host.Stopper | undefined,
	): Promise<tg.Host.WaitOutput>;

	write(fd: number, bytes: Uint8Array): Promise<void>;

	writeSync(fd: number, bytes: Uint8Array): void;
};

export namespace Host {
	export type Signal = "sigwinch";

	export type Stopper = number;

	export type Stdio = "inherit" | "null" | "pipe";

	export type SignalListener = AsyncIterable<void> & {
		close(): Promise<void>;
	};

	export type SpawnArg = {
		executable: string;
		args: Array<string>;
		cwd?: string | undefined;
		env: { [key: string]: string };
		stdin: tg.Host.Stdio;
		stdout: tg.Host.Stdio;
		stderr: tg.Host.Stdio;
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
