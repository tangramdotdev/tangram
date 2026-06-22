import * as tg from "@tangramdotdev/client";
import { http2 } from "./http2.ts";

export let host: tg.Host = {
	http2,

	checksum(
		input: string | Uint8Array,
		algorithm: tg.Checksum.Algorithm,
	): tg.Checksum {
		return syscall("host_checksum", input, algorithm);
	},

	close(fd: number): Promise<void> {
		return syscall("host_close", fd);
	},

	get current(): string {
		return syscall("host_current");
	},

	disableRawMode(fd: number): Promise<void> {
		return syscall("host_disable_raw_mode", fd);
	},

	enableRawMode(fd: number): Promise<void> {
		return syscall("host_enable_raw_mode", fd);
	},

	exec(arg: tg.Host.SpawnArg): Promise<never> {
		return syscall("host_exec", arg);
	},

	exists(path: string): Promise<boolean> {
		return syscall("host_exists", path);
	},

	getTtySize(): tg.Process.Tty.Size | undefined {
		return syscall("host_get_tty_size");
	},

	getxattr(path: string, name: string): Promise<Uint8Array | undefined> {
		return syscall("host_get_xattr", path, name);
	},

	isForegroundControllingTty(fd: number): boolean {
		return syscall("host_is_foreground_controlling_tty", fd);
	},

	isTty(fd: number): boolean {
		return syscall("host_is_tty", fd);
	},

	listenSignal(signal: tg.Host.Signal): tg.Host.SignalListener {
		let closed = false;
		let tokenPromise = syscall("host_signal_open", signal);
		return {
			async close(): Promise<void> {
				if (closed) {
					return;
				}
				closed = true;
				let token = await tokenPromise;
				await syscall("host_signal_close", token);
			},

			async *[Symbol.asyncIterator](): AsyncIterator<void> {
				let token = await tokenPromise;
				try {
					while (!closed) {
						let value = await syscall("host_signal_read", token);
						if (!value) {
							break;
						}
						yield undefined;
					}
				} finally {
					if (!closed) {
						closed = true;
						await syscall("host_signal_close", token);
					}
				}
			},
		};
	},

	magic(value: Function): tg.Command.Data.Executable {
		return syscall("host_magic", value);
	},

	mkdtemp(): Promise<string> {
		return syscall("host_mkdtemp");
	},

	objectId(object: tg.Object.Data): tg.Object.Id {
		return syscall("host_object_id", object);
	},

	get parallelism(): number {
		return syscall("host_parallelism");
	},

	parseValue(value: string): tg.Value.Data {
		return syscall("host_value_parse", value);
	},

	read(
		fd: number,
		length?: number | undefined,
		stopper?: tg.Host.Stopper | undefined,
	): Promise<Uint8Array | undefined> {
		return syscall("host_read", fd, length, stopper);
	},

	remove(path: string): Promise<void> {
		return syscall("host_remove", path);
	},

	signal(pid: number, signal: tg.Process.Signal): Promise<void> {
		return syscall("host_signal", pid, signal);
	},

	stringifyValue(value: tg.Value.Data): string {
		return syscall("host_value_stringify", value);
	},

	sleep(
		duration: number,
		stopper?: tg.Host.Stopper | undefined,
	): Promise<void> {
		return syscall("host_sleep", duration, stopper);
	},

	spawn(arg: tg.Host.SpawnArg): Promise<tg.Host.SpawnOutput> {
		return syscall("host_spawn", arg);
	},

	stopperClose(stopper: tg.Host.Stopper): Promise<void> {
		return syscall("host_stopper_close", stopper);
	},

	stopperOpen(): Promise<tg.Host.Stopper> {
		return syscall("host_stopper_open");
	},

	stopperStop(stopper: tg.Host.Stopper): Promise<void> {
		return syscall("host_stopper_stop", stopper);
	},

	wait(
		pid: number,
		stopper?: tg.Host.Stopper | undefined,
	): Promise<tg.Host.WaitOutput> {
		return syscall("host_wait", pid, stopper);
	},

	write(fd: number, bytes: Uint8Array): Promise<void> {
		return syscall("host_write", fd, bytes);
	},

	writeSync(fd: number, bytes: Uint8Array): void {
		syscall("host_write_sync", fd, bytes);
	},
};
