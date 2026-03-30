import type * as tg from "@tangramdotdev/client";

export let host: tg.Host = {
	close(fd: number): Promise<void> {
		return syscall("host_close", fd);
	},

	exists(path: string): Promise<boolean> {
		return syscall("host_exists", path);
	},

	getTtySize(): tg.Process.Tty.Size | undefined {
		return syscall("host_get_tty_size", undefined);
	},

	getxattr(path: string, name: string): Promise<Uint8Array | undefined> {
		return syscall("host_get_xattr", path, name);
	},

	isTty(fd: number): boolean {
		return syscall("host_is_tty", fd);
	},

	listenSignal(signal: tg.Host.Signal): tg.Host.SignalListener {
		return listenSignal(signal);
	},

	magic(value: Function): tg.Command.Data.Executable {
		return syscall("host_magic", value);
	},

	mkdtemp(): Promise<string> {
		return syscall("host_mkdtemp", undefined);
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

	sleep(
		duration: number,
		stopper?: tg.Host.Stopper | undefined,
	): Promise<void> {
		return syscall("host_sleep", duration, stopper);
	},

	spawn(arg: tg.Host.SpawnArg): Promise<tg.Host.SpawnOutput> {
		return syscall("host_spawn", arg);
	},

	stopClose(stopper: tg.Host.Stopper): Promise<void> {
		return syscall("host_stop_close", stopper);
	},

	stopOpen(): Promise<tg.Host.Stopper> {
		return syscall("host_stop_open", undefined);
	},

	stopStop(stopper: tg.Host.Stopper): Promise<void> {
		return syscall("host_stop_stop", stopper);
	},

	stdin(length?: number | undefined): tg.Host.StdinListener {
		return stdin(length);
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

function listenSignal(signal: tg.Host.Signal): tg.Host.SignalListener {
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
}

function stdin(length?: number | undefined): tg.Host.StdinListener {
	let closed = false;
	let released = false;
	let stopperPromise = syscall("host_stop_open", undefined);
	let release = async () => {
		if (released) {
			return;
		}
		released = true;
		let stopper = await stopperPromise;
		await syscall("host_stop_close", stopper);
	};
	return {
		async close(): Promise<void> {
			if (closed) {
				return;
			}
			closed = true;
			let stopper = await stopperPromise;
			await syscall("host_stop_stop", stopper);
			await release();
		},

		async *[Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
			let stopper = await stopperPromise;
			try {
				while (!closed) {
					let bytes = await syscall("host_read", 0, length, stopper);
					if (bytes === undefined) {
						break;
					}
					if (bytes.length === 0) {
						continue;
					}
					yield bytes;
				}
			} finally {
				closed = true;
				await release();
			}
		},
	};
}
