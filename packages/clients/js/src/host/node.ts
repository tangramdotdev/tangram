import { blake3 } from "@noble/hashes/blake3.js";
import * as childProcess from "node:child_process";
import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as http2 from "node:http2";
import { createRequire } from "node:module";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import type { Readable, Writable } from "node:stream";
import * as tty from "node:tty";
import type { Host } from "../host.ts";
import type * as tg from "../index.ts";

type FileDescriptor = {
	readable?: Readable;
	writable?: Writable;
};

type ProcessState = {
	child: childProcess.ChildProcess;
	result: Promise<ProcessResult>;
};

type ProcessResult = {
	code: number | null;
	signal: NodeJS.Signals | null;
};

type RawModeState = {
	original: boolean;
	stream: tty.ReadStream;
};

type StopperState = {
	controller: AbortController;
};

type Native = {
	objectId(input: string): string;
	parseValue(input: string): string;
	stringifyValue(input: string): string;
};

let fileDescriptors = new Map<number, FileDescriptor>();
let nextToken = 3;
let processes = new Map<number, ProcessState>();
let rawModes = new Map<number, RawModeState>();
let stoppers = new Map<number, StopperState>();
let native: Native | undefined;

let http2Host: Host.Http2 = {
	ClientHttp2Session: function (
		authority: string,
		options?: Host.Http2.ConnectOptions,
	) {
		return connectHttp2(authority, options);
	} as unknown as Host.Http2["ClientHttp2Session"],
	ClientHttp2Stream: function (
		session: Host.Http2.ClientHttp2Session,
		headers: Host.Http2.Headers,
		options: Host.Http2.RequestOptions,
	) {
		return session.request(headers, options);
	} as unknown as Host.Http2["ClientHttp2Stream"],
	connect(authority, options, listener) {
		if (typeof options === "function") {
			let session = connectHttp2(authority.toString());
			session.once("connect", () => options(session));
			return session;
		}
		let session = connectHttp2(authority.toString(), options);
		if (listener !== undefined) {
			session.once("connect", () => listener(session));
		}
		return session;
	},
	constants: {
		HTTP2_HEADER_AUTHORITY: ":authority",
		HTTP2_HEADER_METHOD: ":method",
		HTTP2_HEADER_PATH: ":path",
		HTTP2_HEADER_SCHEME: ":scheme",
		HTTP2_HEADER_STATUS: ":status",
		HTTP2_METHOD_GET: "GET",
		HTTP2_METHOD_POST: "POST",
	},
};

export let host: Host = {
	http2: http2Host,

	checksum(
		input: string | Uint8Array,
		algorithm: tg.Checksum.Algorithm,
	): tg.Checksum {
		let bytes =
			typeof input === "string" ? new TextEncoder().encode(input) : input;
		let digest: Uint8Array;
		if (algorithm === "blake3") {
			digest = blake3(bytes);
		} else {
			digest = crypto.createHash(algorithm).update(bytes).digest();
		}
		return `${algorithm}:${hex(digest)}`;
	},

	async close(fd: number): Promise<void> {
		let descriptor = fileDescriptors.get(fd);
		if (descriptor === undefined) {
			throw new Error(`failed to find the file descriptor ${fd}`);
		}
		fileDescriptors.delete(fd);
		if (descriptor.readable !== undefined) {
			descriptor.readable.destroy();
		}
		if (descriptor.writable !== undefined) {
			await closeWritable(descriptor.writable);
		}
	},

	get current(): string {
		let architecture = process.arch === "arm64" ? "aarch64" : process.arch;
		let platform = process.platform;
		if (
			(architecture !== "aarch64" && architecture !== "x64") ||
			(platform !== "darwin" && platform !== "linux")
		) {
			throw new Error(`unsupported host ${process.arch}-${process.platform}`);
		}
		architecture = architecture === "x64" ? "x86_64" : architecture;
		return `${architecture}-${platform}`;
	},

	async disableRawMode(fd: number): Promise<void> {
		let rawMode = rawModes.get(fd);
		if (rawMode === undefined) {
			return;
		}
		rawModes.delete(fd);
		rawMode.stream.setRawMode(rawMode.original);
	},

	async enableRawMode(fd: number): Promise<void> {
		if (!this.isForegroundControllingTty(fd) || rawModes.has(fd)) {
			return;
		}
		let stream =
			fd === 0 ? (process.stdin as tty.ReadStream) : new tty.ReadStream(fd);
		let original = stream.isRaw;
		stream.setRawMode(true);
		rawModes.set(fd, { original, stream });
	},

	async exec(arg: tg.Host.SpawnArg): Promise<never> {
		validateExecStdio(arg.stdin, "stdin");
		validateExecStdio(arg.stdout, "stdout");
		validateExecStdio(arg.stderr, "stderr");
		let executable = resolveExecutable(arg.executable, arg.env);
		if (
			arg.stdin === "inherit" &&
			arg.stdout === "inherit" &&
			arg.stderr === "inherit"
		) {
			if (arg.cwd !== null) {
				process.chdir(arg.cwd);
			}
			if (process.execve === undefined) {
				throw new Error("the default host requires process.execve");
			}
			process.execve(executable, [executable, ...arg.args], arg.env);
			throw new Error(`failed to exec ${arg.executable}`);
		}
		let options: childProcess.SpawnOptions = {
			...(arg.cwd === null ? {} : { cwd: arg.cwd }),
			env: arg.env,
			stdio: [arg.stdin, arg.stdout, arg.stderr].map(nodeStdio),
		};
		let child: childProcess.ChildProcess = childProcess.spawn(
			executable,
			arg.args,
			options,
		);
		return await new Promise<never>((_, reject) => {
			child.once("error", reject);
			child.once("exit", (code, signal) => {
				if (signal !== null) {
					process.kill(process.pid, signal);
					return;
				}
				process.exit(code ?? 1);
			});
		});
	},

	async exists(path: string): Promise<boolean> {
		try {
			await fs.promises.access(path);
			return true;
		} catch {
			return false;
		}
	},

	getTtySize(): tg.Process.Tty.Size | null {
		for (let stream of [process.stdout, process.stderr]) {
			if (
				stream.isTTY &&
				stream.columns !== undefined &&
				stream.rows !== undefined
			) {
				return { cols: stream.columns, rows: stream.rows };
			}
		}
		return null;
	},

	async getxattr(path: string, name: string): Promise<Uint8Array | null> {
		let executable: string;
		let args: Array<string>;
		if (process.platform === "darwin") {
			executable = "xattr";
			args = ["-px", name, path];
		} else if (process.platform === "linux") {
			executable = "getfattr";
			args = ["--absolute-names", "--name", name, "--only-values", path];
		} else {
			throw new Error(
				`extended attributes are unsupported on ${process.platform}`,
			);
		}
		try {
			let bytes = await execFile(executable, args);
			return process.platform === "darwin" ? decodeHex(bytes) : bytes;
		} catch (error) {
			if (isMissingXattrError(error)) {
				return null;
			}
			throw error;
		}
	},

	isForegroundControllingTty(fd: number): boolean {
		return this.isTty(fd);
	},

	isTty(fd: number): boolean {
		return tty.isatty(fd);
	},

	listenSignal(signal: tg.Host.Signal): tg.Host.SignalListener {
		let closed = false;
		let queued = 0;
		let receivers = new Array<(result: IteratorResult<void>) => void>();
		let event = nodeSignal(signal);
		let listener = () => {
			let receiver = receivers.shift();
			if (receiver === undefined) {
				queued++;
			} else {
				receiver({ done: false, value: undefined });
			}
		};
		let close = async () => {
			if (closed) {
				return;
			}
			closed = true;
			process.off(event, listener);
			for (let receiver of receivers.splice(0)) {
				receiver({ done: true, value: undefined });
			}
		};
		process.on(event, listener);
		return {
			close,
			[Symbol.asyncIterator]() {
				return {
					async next(): Promise<IteratorResult<void>> {
						if (queued > 0) {
							queued--;
							return { done: false, value: undefined };
						}
						if (closed) {
							return { done: true, value: undefined };
						}
						return await new Promise((resolve) => receivers.push(resolve));
					},
					async return(): Promise<IteratorResult<void>> {
						await close();
						return { done: true, value: undefined };
					},
				};
			},
		};
	},

	magic(value: Function): tg.Command.Data.Executable {
		let file = process.argv[1];
		if (file === undefined) {
			throw new Error("failed to find the module for the function");
		}
		file = path.resolve(file);
		let extension = path.extname(file);
		let kind: tg.Module.Kind =
			extension === ".ts" ||
			extension === ".mts" ||
			extension === ".cts" ||
			extension === ".tsx"
				? "ts"
				: "js";
		let module: tg.Module.Data = { kind, referent: file };
		return {
			...(value.name.length > 0 ? { export: value.name } : {}),
			module,
		};
	},

	async mkdtemp(): Promise<string> {
		return await fs.promises.mkdtemp(path.join(os.tmpdir(), "tangram-"));
	},

	objectId(object: tg.Object.Data): tg.Object.Id {
		return getNative().objectId(JSON.stringify(object)) as tg.Object.Id;
	},

	get parallelism(): number {
		return os.availableParallelism();
	},

	parseValue(value: string): tg.Value.Data {
		return JSON.parse(getNative().parseValue(value)) as tg.Value.Data;
	},

	async read(
		fd: number,
		length?: number | null,
		stopper?: tg.Host.Stopper | null,
	): Promise<Uint8Array | null> {
		let descriptor = fileDescriptors.get(fd);
		let readable =
			descriptor?.readable ?? (fd === 0 ? process.stdin : undefined);
		let signal = getStopper(stopper);
		if (readable !== undefined) {
			return await readStream(readable, length ?? 64 * 1024, signal);
		}
		return await readFd(fd, length ?? 64 * 1024, signal);
	},

	async remove(path: string): Promise<void> {
		await fs.promises.rm(path, { force: true, recursive: true });
	},

	async signal(pid: number, signal: tg.Process.Signal): Promise<void> {
		process.kill(pid, `SIG${signal}`);
	},

	async sleep(
		duration: number,
		stopper?: tg.Host.Stopper | null,
	): Promise<void> {
		let signal = getStopper(stopper);
		await new Promise<void>((resolve, reject) => {
			let onTimeout = () => {
				signal?.removeEventListener("abort", onAbort);
				resolve();
			};
			let timeout = setTimeout(onTimeout, duration * 1000);
			if (signal === undefined) {
				return;
			}
			let onAbort = () => {
				clearTimeout(timeout);
				reject(stoppedError());
			};
			if (signal.aborted) {
				onAbort();
			} else {
				signal.addEventListener("abort", onAbort, { once: true });
			}
		});
	},

	async spawn(arg: tg.Host.SpawnArg): Promise<tg.Host.SpawnOutput> {
		let executable = resolveExecutable(arg.executable, arg.env);
		let options: childProcess.SpawnOptions = {
			...(arg.cwd === null ? {} : { cwd: arg.cwd }),
			env: arg.env,
			stdio: [arg.stdin, arg.stdout, arg.stderr].map(nodeStdio),
		};
		let child: childProcess.ChildProcess = childProcess.spawn(
			executable,
			arg.args,
			options,
		);
		let result = new Promise<ProcessResult>((resolve, reject) => {
			child.once("error", reject);
			child.once("exit", (code, signal) => resolve({ code, signal }));
		});
		result.catch(() => undefined);
		await new Promise<void>((resolve, reject) => {
			let onError = (error: Error) => {
				child.off("spawn", onSpawn);
				reject(error);
			};
			let onSpawn = () => {
				child.off("error", onError);
				resolve();
			};
			child.once("error", onError);
			child.once("spawn", onSpawn);
		});
		let pid = child.pid;
		if (pid === undefined) {
			throw new Error("failed to get the process id");
		}
		let stdin =
			child.stdin === null
				? null
				: addFileDescriptor({ writable: child.stdin });
		let stdout =
			child.stdout === null
				? null
				: addFileDescriptor({ readable: child.stdout });
		let stderr =
			child.stderr === null
				? null
				: addFileDescriptor({ readable: child.stderr });
		processes.set(pid, { child, result });
		return { pid, stderr, stdin, stdout };
	},

	async stopperClose(stopper: tg.Host.Stopper): Promise<void> {
		let state = stoppers.get(stopper);
		if (state === undefined) {
			throw new Error(`failed to find the stopper ${stopper}`);
		}
		stoppers.delete(stopper);
		state.controller.abort();
	},

	async stopperOpen(): Promise<tg.Host.Stopper> {
		let token = nextToken++;
		stoppers.set(token, { controller: new AbortController() });
		return token;
	},

	async stopperStop(stopper: tg.Host.Stopper): Promise<void> {
		let state = stoppers.get(stopper);
		if (state === undefined) {
			throw new Error(`failed to find the stopper ${stopper}`);
		}
		state.controller.abort();
	},

	stringifyValue(value: tg.Value.Data): string {
		return getNative().stringifyValue(JSON.stringify(value));
	},

	async wait(
		pid: number,
		stopper?: tg.Host.Stopper | null,
	): Promise<tg.Host.WaitOutput> {
		let process = processes.get(pid);
		if (process === undefined) {
			throw new Error(`failed to find the process ${pid}`);
		}
		processes.delete(pid);
		let signal = getStopper(stopper);
		let onAbort = () => process.child.kill("SIGKILL");
		if (signal?.aborted) {
			onAbort();
		} else {
			signal?.addEventListener("abort", onAbort, { once: true });
		}
		let result: ProcessResult;
		try {
			result = await process.result;
		} finally {
			signal?.removeEventListener("abort", onAbort);
		}
		if (result.code !== null) {
			return { exit: result.code };
		}
		if (result.signal !== null) {
			return { exit: 128 + os.constants.signals[result.signal] };
		}
		throw new Error("failed to determine the exit status");
	},

	async write(fd: number, bytes: Uint8Array): Promise<void> {
		let writable = fileDescriptors.get(fd)?.writable;
		if (writable !== undefined) {
			await writeStream(writable, bytes);
			return;
		}
		await writeFd(fd, bytes);
	},

	writeSync(fd: number, bytes: Uint8Array): void {
		let position = 0;
		while (position < bytes.length) {
			position += fs.writeSync(fd, bytes, position, bytes.length - position);
		}
	},
};

function addFileDescriptor(descriptor: FileDescriptor): number {
	let token = nextToken++;
	fileDescriptors.set(token, descriptor);
	return token;
}

function getNative(): Native {
	if (native !== undefined) {
		return native;
	}
	let name = `tangram_client.${process.platform}-${process.arch}.node`;
	let require = createRequire(import.meta.url);
	native = require(path.join(import.meta.dirname, name)) as Native;

	return native;
}

function connectHttp2(
	authority: string,
	options?: Host.Http2.ConnectOptions,
): Host.Http2.ClientHttp2Session {
	let session: http2.ClientHttp2Session;
	if (authority.startsWith("http+unix://")) {
		let path = decodeURIComponent(authority.slice("http+unix://".length));
		session = http2.connect("http://localhost", {
			createConnection: () => net.connect(path),
		});
	} else {
		let url = new URL(authority);
		if (options?.port !== undefined) {
			url.port = options.port.toString();
		}
		session = http2.connect(url);
	}
	return session as unknown as Host.Http2.ClientHttp2Session;
}

async function closeWritable(stream: Writable): Promise<void> {
	if (stream.destroyed || stream.writableEnded) {
		return;
	}
	await new Promise<void>((resolve, reject) => {
		stream.end((error?: Error | null) => {
			if (error === undefined || error === null) {
				resolve();
			} else {
				reject(error);
			}
		});
	});
}

async function execFile(
	executable: string,
	args: Array<string>,
): Promise<Uint8Array> {
	return await new Promise((resolve, reject) => {
		childProcess.execFile(
			executable,
			args,
			{ encoding: null },
			(error, stdout, stderr) => {
				if (error !== null) {
					Object.defineProperty(error, "stderr", { value: stderr });
					reject(error);
				} else {
					resolve(stdout);
				}
			},
		);
	});
}

function getStopper(stopper?: tg.Host.Stopper | null): AbortSignal | undefined {
	if (stopper === undefined || stopper === null) {
		return undefined;
	}
	let state = stoppers.get(stopper);
	if (state === undefined) {
		throw new Error(`failed to find the stopper ${stopper}`);
	}
	return state.controller.signal;
}

function decodeHex(bytes: Uint8Array): Uint8Array {
	let string = new TextDecoder().decode(bytes).replaceAll(/\s/g, "");
	if (string.length % 2 !== 0 || !/^[0-9a-f]*$/i.test(string)) {
		throw new Error("failed to decode the extended attribute");
	}
	let output = new Uint8Array(string.length / 2);
	for (let index = 0; index < output.length; index++) {
		output[index] = Number.parseInt(string.slice(index * 2, index * 2 + 2), 16);
	}
	return output;
}

function hex(bytes: Uint8Array): string {
	return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(
		"",
	);
}

function isMissingXattrError(error: unknown): boolean {
	if (!(error instanceof Error) || !("stderr" in error)) {
		return false;
	}
	let stderr = error.stderr;
	let message =
		typeof stderr === "string"
			? stderr
			: stderr instanceof Uint8Array
				? new TextDecoder().decode(stderr)
				: "";
	return /no such (attribute|xattr)/i.test(message);
}

function nodeSignal(signal: tg.Host.Signal): NodeJS.Signals {
	if (signal === "sigwinch") {
		return "SIGWINCH";
	}
	throw new Error(`unsupported signal ${signal}`);
}

function nodeStdio(stdio: tg.Host.Stdio): "ignore" | "inherit" | "pipe" {
	return stdio === "null" ? "ignore" : stdio;
}

async function readFd(
	fd: number,
	length: number,
	signal: AbortSignal | undefined,
): Promise<Uint8Array | null> {
	if (signal?.aborted) {
		throw stoppedError();
	}
	let buffer = new Uint8Array(length);
	let read = new Promise<Uint8Array | null>((resolve, reject) => {
		fs.read(fd, buffer, 0, length, null, (error, bytesRead) => {
			if (error !== null) {
				reject(error);
			} else if (bytesRead === 0) {
				resolve(null);
			} else {
				resolve(buffer.subarray(0, bytesRead));
			}
		});
	});
	return await withSignal(read, signal);
}

async function readStream(
	stream: Readable,
	length: number,
	signal: AbortSignal | undefined,
): Promise<Uint8Array | null> {
	if (signal?.aborted) {
		throw stoppedError();
	}
	return await new Promise((resolve, reject) => {
		let cleanup = () => {
			stream.off("close", onEnd);
			stream.off("end", onEnd);
			stream.off("error", onError);
			stream.off("readable", onReadable);
			signal?.removeEventListener("abort", onAbort);
		};
		let finish = (value: Uint8Array | null) => {
			cleanup();
			resolve(value);
		};
		let onAbort = () => {
			cleanup();
			reject(stoppedError());
		};
		let onEnd = () => finish(readChunk(stream, length));
		let onError = (error: Error) => {
			cleanup();
			reject(error);
		};
		let onReadable = () => {
			let bytes = readChunk(stream, length);
			if (bytes !== null) {
				finish(bytes);
			}
		};
		let bytes = readChunk(stream, length);
		if (bytes !== null) {
			finish(bytes);
			return;
		}
		if (stream.readableEnded || stream.destroyed) {
			finish(null);
			return;
		}
		stream.once("close", onEnd);
		stream.once("end", onEnd);
		stream.once("error", onError);
		stream.on("readable", onReadable);
		signal?.addEventListener("abort", onAbort, { once: true });
	});
}

function readChunk(stream: Readable, length: number): Uint8Array | null {
	let value: unknown = stream.read();
	if (value === null) {
		return null;
	}
	let bytes =
		value instanceof Uint8Array
			? value
			: new TextEncoder().encode(String(value));
	if (bytes.length <= length) {
		return bytes;
	}
	stream.unshift(bytes.subarray(length));
	return bytes.subarray(0, length);
}

function resolveExecutable(
	executable: string,
	env: Record<string, string>,
): string {
	if (path.isAbsolute(executable) || executable.includes(path.sep)) {
		return executable;
	}
	let pathValue = env.PATH;
	if (pathValue === undefined) {
		throw new Error(`failed to find ${executable} in PATH`);
	}
	for (let directory of pathValue.split(path.delimiter)) {
		let candidate = path.join(directory, executable);
		try {
			if (fs.statSync(candidate).isFile()) {
				return candidate;
			}
		} catch {
			continue;
		}
	}
	throw new Error(`failed to find ${executable} in PATH`);
}

function stoppedError(): Error {
	return new Error("the operation was stopped");
}

function validateExecStdio(
	stdio: tg.Host.Stdio,
	stream: "stdin" | "stdout" | "stderr",
): void {
	if (stdio === "inherit" || stdio === "null") {
		return;
	}
	throw new Error(`${stream} must be inherit or null for an exec`);
}

async function withSignal<T>(
	promise: Promise<T>,
	signal: AbortSignal | undefined,
): Promise<T> {
	if (signal === undefined) {
		return await promise;
	}
	if (signal.aborted) {
		throw stoppedError();
	}
	return await new Promise((resolve, reject) => {
		let onAbort = () => reject(stoppedError());
		signal.addEventListener("abort", onAbort, { once: true });
		promise
			.then(resolve, reject)
			.finally(() => signal.removeEventListener("abort", onAbort));
	});
}

async function writeFd(fd: number, bytes: Uint8Array): Promise<void> {
	let position = 0;
	while (position < bytes.length) {
		let bytesWritten = await new Promise<number>((resolve, reject) => {
			fs.write(
				fd,
				bytes,
				position,
				bytes.length - position,
				null,
				(error, count) => (error === null ? resolve(count) : reject(error)),
			);
		});
		position += bytesWritten;
	}
}

async function writeStream(stream: Writable, bytes: Uint8Array): Promise<void> {
	await new Promise<void>((resolve, reject) => {
		stream.write(bytes, (error) =>
			error === undefined || error === null ? resolve() : reject(error),
		);
	});
}
