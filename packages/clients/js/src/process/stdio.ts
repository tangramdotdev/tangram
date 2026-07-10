import * as tg from "../index.ts";

export type Stdio = "inherit" | "log" | "null" | "pipe" | "tty";

export namespace Stdio {
	export type Chunk = {
		bytes: Uint8Array;
		position?: number | null;
		stream: tg.Process.Stdio.Stream;
	};

	export type Stream = "stdin" | "stdout" | "stderr";

	export namespace Read {
		export type Arg = {
			length?: number | null;
			location?: tg.Location.Arg | null;
			position?: number | string | null;
			size?: number | null;
			streams: Array<tg.Process.Stdio.Stream>;
			timeout?: number | null;
		};

		export type Event =
			| { kind: "chunk"; value: tg.Process.Stdio.Chunk }
			| { kind: "end" };

		export namespace Event {
			export type Data =
				| { kind: "chunk"; value: tg.Process.Stdio.Read.Event.Data.Chunk }
				| { kind: "end" };

			export namespace Data {
				export type Chunk = {
					bytes: string;
					position?: number | null;
					stream: tg.Process.Stdio.Stream;
				};
			}

			export let fromData = (
				data: tg.Process.Stdio.Read.Event.Data,
			): tg.Process.Stdio.Read.Event => {
				if (data.kind === "chunk") {
					return {
						kind: "chunk",
						value: {
							bytes: tg.encoding.base64.decode(data.value.bytes),
							position: data.value.position ?? null,
							stream: data.value.stream,
						},
					};
				} else {
					return data;
				}
			};

			export let toData = (
				event: tg.Process.Stdio.Read.Event,
			): tg.Process.Stdio.Read.Event.Data => {
				if (event.kind === "chunk") {
					return {
						kind: "chunk",
						value: {
							bytes: tg.encoding.base64.encode(event.value.bytes),
							...(event.value.position !== undefined &&
							event.value.position !== null
								? { position: event.value.position }
								: {}),
							stream: event.value.stream,
						},
					};
				} else {
					return event;
				}
			};
		}
	}

	export namespace Write {
		export type Arg = {
			location?: tg.Location.Arg | null;
			streams: Array<tg.Process.Stdio.Stream>;
		};

		export type Event = { kind: "end" } | { kind: "stop" };
	}

	export class Reader {
		#available: boolean;
		#fd: number | null;
		#input: AsyncIterableIterator<tg.Process.Stdio.Read.Event> | null;
		#process: tg.Process | null;
		#stream: "stdout" | "stderr";

		constructor(arg: {
			fd?: number | null;
			unavailable?: boolean;
			stream: "stdout" | "stderr";
		}) {
			this.#available = !(arg.unavailable ?? false);
			this.#fd = arg.fd ?? null;
			this.#input = null;
			this.#process = null;
			this.#stream = arg.stream;
		}

		setProcess(process: tg.Process): void {
			this.#process = process;
		}

		/** Close the stream without reading it. */
		async close(): Promise<void> {
			let fd = this.#fd;
			let input = this.#input;
			this.#fd = null;
			this.#input = null;
			this.#process = null;
			if (!this.#available) {
				return;
			}
			if (fd !== null) {
				await tg.host.close(fd);
			}
			if (input !== null) {
				await input.return?.();
			}
		}

		/** Read one chunk from the stream. */
		async read(): Promise<Uint8Array | null> {
			if (!this.#available) {
				throw new Error(`${this.#stream} is not available`);
			}
			if (this.#fd !== null) {
				while (true) {
					let bytes = await tg.host.read(this.#fd, 4096);
					if (bytes === null) {
						break;
					}
					if (bytes.length > 0) {
						return bytes;
					}
				}
				let fd = this.#fd;
				this.#fd = null;
				this.#process = null;
				if (fd !== null) {
					await tg.host.close(fd);
				}
				return null;
			}
			if (this.#process === null) {
				throw new Error(`${this.#stream} is not available`);
			}
			if (this.#input === null) {
				if (typeof this.#process.id !== "string") {
					throw new Error("expected a sandboxed process id");
				}
				let input = await tg.client.tryReadProcessStdio(this.#process.id, {
					...(this.#process.location !== null
						? { location: this.#process.location }
						: {}),
					streams: [this.#stream],
				});
				if (input === null) {
					throw new Error(`${this.#stream} is not available`);
				}
				this.#input = input;
			}
			while (true) {
				let result = await this.#input.next();
				if (result.done) {
					break;
				}
				let event = result.value;
				if (event.kind === "end") {
					break;
				}
				if (event.value.stream !== this.#stream) {
					throw new Error("invalid process stdio stream");
				}
				if (event.value.bytes.length > 0) {
					return event.value.bytes;
				}
			}
			this.#input = null;
			this.#process = null;
			return null;
		}

		/** Read all remaining bytes from the stream. */
		async readAll(): Promise<Uint8Array> {
			let chunks: Array<Uint8Array> = [];
			let length = 0;
			while (true) {
				let bytes = await this.read();
				if (bytes === null) {
					break;
				}
				chunks.push(bytes);
				length += bytes.length;
			}
			let output = new Uint8Array(length);
			let position = 0;
			for (let chunk of chunks) {
				output.set(chunk, position);
				position += chunk.length;
			}
			return output;
		}

		/** Read all remaining bytes as UTF-8 text. */
		async readAllToString(): Promise<string> {
			return tg.encoding.utf8.decode(await this.readAll());
		}

		/** Read all remaining bytes as UTF-8 text. */
		async text(): Promise<string> {
			return this.readAllToString();
		}
	}

	export class Writer {
		#available: boolean;
		#fd: number | null;
		#process: tg.Process | null;
		#stream: "stdin";

		constructor(arg: {
			fd?: number | null;
			unavailable?: boolean;
			stream: "stdin";
		}) {
			this.#available = !(arg.unavailable ?? false);
			this.#fd = arg.fd ?? null;
			this.#process = null;
			this.#stream = arg.stream;
		}

		setProcess(process: tg.Process): void {
			this.#process = process;
		}

		/** Close the stream without writing additional input. */
		async close(): Promise<void> {
			let fd = this.#fd;
			let process = this.#process;
			let stream = this.#stream;
			if (!this.#available) {
				this.#fd = null;
				this.#process = null;
				return;
			}
			if (fd !== null) {
				this.#fd = null;
				this.#process = null;
				await tg.host.close(fd);
				return;
			}
			if (process !== null) {
				if (typeof process.id === "number") {
					this.#fd = null;
					this.#process = null;
					return;
				}
				let location = process.location;
				if (location === null) {
					await process.load();
					location = process.location;
				}
				this.#fd = null;
				this.#process = null;
				if (typeof process.id !== "string") {
					throw new Error("expected a sandboxed process id");
				}
				await consumeWriteEvents(
					await tg.client.writeProcessStdio(
						process.id,
						{
							...(location !== null ? { location } : {}),
							streams: [stream],
						},
						(async function* () {
							yield { kind: "end" };
						})(),
					),
				);
			}
		}

		/** Write one chunk to the stream and return the number of bytes written. */
		async write(input: Uint8Array): Promise<number> {
			if (!(input instanceof Uint8Array)) {
				throw new Error("expected stdio bytes");
			}
			if (!this.#available) {
				throw new Error(`${this.#stream} is not available`);
			}
			let fd = this.#fd;
			let process = this.#process;
			let stream = this.#stream;
			if (fd === null && process === null) {
				throw new Error(`${stream} is not available`);
			}
			if (input.length === 0) {
				return 0;
			}
			if (fd !== null) {
				await tg.host.write(fd, input);
				return input.length;
			}
			if (typeof process!.id === "number") {
				throw new Error(`${stream} is not available`);
			}
			let location = process!.location;
			if (location === null) {
				await process!.load();
				location = process!.location;
			}
			await consumeWriteEvents(
				await tg.client.writeProcessStdio(
					process!.id,
					{
						...(location !== null ? { location } : {}),
						streams: [stream],
					},
					(async function* () {
						yield {
							kind: "chunk",
							value: {
								bytes: input,
								stream,
							},
						};
					})(),
				),
			);
			return input.length;
		}

		/** Write all input bytes to the stream and then close it. */
		async writeAll(input: Uint8Array): Promise<void> {
			if (!(input instanceof Uint8Array)) {
				throw new Error("expected stdio bytes");
			}
			let position = 0;
			while (position < input.length) {
				let count = await this.write(input.subarray(position));
				if (count === 0) {
					throw new Error("failed to write stdin");
				}
				position += count;
			}
			await this.close();
		}
	}
}

export let task = async (
	id: tg.Process.Id,
	location: tg.Location.Arg | null,
	stdin: "pipe" | "tty" | null,
	stdout: "pipe" | "tty" | null,
	stderr: "pipe" | "tty" | null,
	tty: boolean,
): Promise<void> => {
	let stdinError: unknown = null;
	let stdinFailed = false;
	let stdinClosing = false;
	let stdinStopper = stdin !== null ? await tg.host.stopperOpen() : null;
	let stdinTask_ =
		stdin !== null && stdinStopper !== null
			? stdinTask(id, location, stdin, stdinStopper).catch((error) => {
					if (!stdinClosing) {
						stdinError = error;
						stdinFailed = true;
					}
				})
			: null;
	let sigwinchError: unknown = null;
	let sigwinchFailed = false;
	let sigwinchListener = tty ? tg.host.listenSignal("sigwinch") : null;
	let sigwinchTask_ =
		sigwinchListener !== null
			? sigwinchTask(id, location, sigwinchListener).catch((error) => {
					sigwinchError = error;
					sigwinchFailed = true;
				})
			: null;
	let stdoutStderrError: unknown = null;
	let stdoutStderrFailed = false;
	try {
		try {
			await stdoutStderrTask(id, location, stdout, stderr);
		} catch (error) {
			stdoutStderrError = error;
			stdoutStderrFailed = true;
		}
		if (stdinStopper !== null) {
			stdinClosing = true;
			await tg.host.stopperStop(stdinStopper);
		}
	} finally {
		await cleanup(stdinStopper, sigwinchListener);
	}
	if (stdinTask_ !== null) {
		await stdinTask_;
	}
	if (sigwinchTask_ !== null) {
		await sigwinchTask_;
	}
	if (stdinFailed) {
		throw stdinError;
	}
	if (sigwinchFailed) {
		throw sigwinchError;
	}
	if (stdoutStderrFailed) {
		throw stdoutStderrError;
	}
};

async function cleanup(
	stdinStopper: tg.Host.Stopper | null,
	sigwinchListener: tg.Host.SignalListener | null,
): Promise<void> {
	try {
		if (sigwinchListener !== null) {
			await sigwinchListener.close();
		}
	} finally {
		if (stdinStopper !== null) {
			await tg.host.stopperClose(stdinStopper);
		}
	}
}

async function stdinTask(
	id: tg.Process.Id,
	location: tg.Location.Arg | null,
	stdin: "pipe" | "tty",
	stopper: tg.Host.Stopper,
): Promise<void> {
	let error: unknown = null;
	let failed = false;
	let raw = stdin === "tty" && tg.host.isForegroundControllingTty(0);
	if (raw) {
		await tg.host.enableRawMode(0);
	}
	try {
		let input =
			(async function* (): AsyncIterableIterator<tg.Process.Stdio.Read.Event> {
				while (true) {
					let bytes = await tg.host.read(0, 4096, stopper);
					if (bytes === null) {
						break;
					}
					if (bytes.length === 0) {
						continue;
					}
					yield {
						kind: "chunk",
						value: {
							bytes,
							stream: "stdin",
						},
					};
				}
				yield { kind: "end" };
			})();
		await consumeWriteEvents(
			await tg.client.writeProcessStdio(
				id,
				{
					...(location !== null ? { location } : {}),
					streams: ["stdin"],
				},
				input,
			),
		);
	} catch (error_) {
		error = error_;
		failed = true;
	} finally {
		if (raw) {
			try {
				await tg.host.disableRawMode(0);
			} catch (disableError) {
				if (!failed) {
					error = disableError;
					failed = true;
				}
			}
		}
	}
	if (failed) {
		throw error;
	}
}

async function stdoutStderrTask(
	id: tg.Process.Id,
	location: tg.Location.Arg | null,
	stdout: "pipe" | "tty" | null,
	stderr: "pipe" | "tty" | null,
): Promise<void> {
	let streams: Array<tg.Process.Stdio.Stream> = [];
	if (stdout !== null) {
		streams.push("stdout");
	}
	if (stderr !== null) {
		streams.push("stderr");
	}
	if (streams.length === 0) {
		return;
	}
	let iterator = await tg.client.tryReadProcessStdio(id, {
		...(location !== null ? { location } : {}),
		streams,
	});
	if (iterator === null) {
		return;
	}
	for await (let event of iterator) {
		if (event.kind === "end") {
			break;
		}
		let fd = event.value.stream === "stdout" ? 1 : 2;
		await tg.host.write(fd, event.value.bytes);
	}
}

async function sigwinchTask(
	id: tg.Process.Id,
	location: tg.Location.Arg | null,
	signalListener: tg.Host.SignalListener,
): Promise<void> {
	for await (let _ of signalListener) {
		let size = tg.host.getTtySize();
		if (size === null) {
			continue;
		}
		let arg: tg.Process.Tty.Put.Arg = { size };
		if (location !== null) {
			arg.location = location;
		}
		await tg.client.setProcessTtySize(id, arg);
	}
}

async function consumeWriteEvents(
	events: AsyncIterable<tg.Process.Stdio.Write.Event>,
): Promise<void> {
	for await (let event of events) {
		if (event.kind === "end") {
			break;
		}
	}
}
