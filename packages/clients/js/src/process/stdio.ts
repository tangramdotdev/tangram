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
							...(event.value.position != null
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
		#fd: number | undefined;
		#input: AsyncIterableIterator<tg.Process.Stdio.Read.Event> | undefined;
		#process: tg.Process | undefined;
		#stream: "stdout" | "stderr";

		constructor(arg: {
			fd?: number;
			unavailable?: boolean;
			stream: "stdout" | "stderr";
		}) {
			this.#available = !(arg.unavailable ?? false);
			this.#fd = arg.fd;
			this.#input = undefined;
			this.#process = undefined;
			this.#stream = arg.stream;
		}

		setProcess(process: tg.Process): void {
			this.#process = process;
		}

		/** Close the stream without reading it. */
		async close(): Promise<void> {
			let fd = this.#fd;
			let input = this.#input;
			this.#fd = undefined;
			this.#input = undefined;
			this.#process = undefined;
			if (!this.#available) {
				return;
			}
			if (fd !== undefined) {
				await tg.host.close(fd);
			}
			if (input !== undefined) {
				await input.return?.();
			}
		}

		/** Read one chunk from the stream. */
		async read(): Promise<Uint8Array | undefined> {
			if (!this.#available) {
				throw new Error(`${this.#stream} is not available`);
			}
			if (this.#fd !== undefined) {
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
				this.#fd = undefined;
				this.#process = undefined;
				if (fd !== undefined) {
					await tg.host.close(fd);
				}
				return undefined;
			}
			if (this.#process === undefined) {
				throw new Error(`${this.#stream} is not available`);
			}
			if (this.#input === undefined) {
				if (typeof this.#process.id !== "string") {
					throw new Error("expected a sandboxed process id");
				}
				let input = await tg.client.tryReadProcessStdio(this.#process.id, {
					...(this.#process.location !== undefined
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
			this.#input = undefined;
			this.#process = undefined;
			return undefined;
		}

		/** Read all remaining bytes from the stream. */
		async readAll(): Promise<Uint8Array> {
			let chunks: Array<Uint8Array> = [];
			let length = 0;
			while (true) {
				let bytes = await this.read();
				if (bytes === undefined) {
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
		#fd: number | undefined;
		#process: tg.Process | undefined;
		#stream: "stdin";

		constructor(arg: { fd?: number; unavailable?: boolean; stream: "stdin" }) {
			this.#available = !(arg.unavailable ?? false);
			this.#fd = arg.fd;
			this.#process = undefined;
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
				this.#fd = undefined;
				this.#process = undefined;
				return;
			}
			if (fd !== undefined) {
				this.#fd = undefined;
				this.#process = undefined;
				await tg.host.close(fd);
				return;
			}
			if (process !== undefined) {
				if (typeof process.id === "number") {
					this.#fd = undefined;
					this.#process = undefined;
					return;
				}
				let location = process.location;
				if (location === undefined) {
					await process.load();
					location = process.location;
				}
				this.#fd = undefined;
				this.#process = undefined;
				if (typeof process.id !== "string") {
					throw new Error("expected a sandboxed process id");
				}
				await consumeWriteEvents(
					await tg.client.writeProcessStdio(
						process.id,
						{
							...(location !== undefined ? { location } : {}),
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
			if (fd === undefined && process === undefined) {
				throw new Error(`${stream} is not available`);
			}
			if (input.length === 0) {
				return 0;
			}
			if (fd !== undefined) {
				await tg.host.write(fd, input);
				return input.length;
			}
			if (typeof process!.id === "number") {
				throw new Error(`${stream} is not available`);
			}
			let location = process!.location;
			if (location === undefined) {
				await process!.load();
				location = process!.location;
			}
			await consumeWriteEvents(
				await tg.client.writeProcessStdio(
					process!.id,
					{
						...(location !== undefined ? { location } : {}),
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
	location: tg.Location.Arg | undefined,
	stdin: "pipe" | "tty" | undefined,
	stdout: "pipe" | "tty" | undefined,
	stderr: "pipe" | "tty" | undefined,
	tty: boolean,
): Promise<void> => {
	let stdinError: unknown;
	let stdinClosing = false;
	let stdinStopper =
		stdin !== undefined ? await tg.host.stopperOpen() : undefined;
	let stdinTask_ =
		stdin !== undefined && stdinStopper !== undefined
			? stdinTask(id, location, stdin, stdinStopper).catch((error) => {
					if (!stdinClosing) {
						stdinError = error;
					}
				})
			: undefined;
	let sigwinchError: unknown;
	let sigwinchListener = tty ? tg.host.listenSignal("sigwinch") : undefined;
	let sigwinchTask_ =
		sigwinchListener !== undefined
			? sigwinchTask(id, location, sigwinchListener).catch((error) => {
					sigwinchError = error;
				})
			: undefined;
	let stdoutStderrError: unknown;
	try {
		stdoutStderrError = await stdoutStderrTask(
			id,
			location,
			stdout,
			stderr,
		).then(
			() => undefined,
			(error) => error,
		);
		if (stdinStopper !== undefined) {
			stdinClosing = true;
			await tg.host.stopperStop(stdinStopper);
		}
	} finally {
		await cleanup(stdinStopper, sigwinchListener);
	}
	if (stdinTask_ !== undefined) {
		await stdinTask_;
	}
	if (sigwinchTask_ !== undefined) {
		await sigwinchTask_;
	}
	if (stdinError !== undefined) {
		throw stdinError;
	}
	if (sigwinchError !== undefined) {
		throw sigwinchError;
	}
	if (stdoutStderrError !== undefined) {
		throw stdoutStderrError;
	}
};

async function cleanup(
	stdinStopper: tg.Host.Stopper | undefined,
	sigwinchListener: tg.Host.SignalListener | undefined,
): Promise<void> {
	try {
		if (sigwinchListener !== undefined) {
			await sigwinchListener.close();
		}
	} finally {
		if (stdinStopper !== undefined) {
			await tg.host.stopperClose(stdinStopper);
		}
	}
}

async function stdinTask(
	id: tg.Process.Id,
	location: tg.Location.Arg | undefined,
	stdin: "pipe" | "tty",
	stopper: tg.Host.Stopper,
): Promise<void> {
	let error: unknown;
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
					...(location !== undefined ? { location } : {}),
					streams: ["stdin"],
				},
				input,
			),
		);
	} catch (error_) {
		error = error_;
	} finally {
		if (raw) {
			try {
				await tg.host.disableRawMode(0);
			} catch (disableError) {
				if (error === undefined) {
					error = disableError;
				}
			}
		}
	}
	if (error !== undefined) {
		throw error;
	}
}

async function stdoutStderrTask(
	id: tg.Process.Id,
	location: tg.Location.Arg | undefined,
	stdout: "pipe" | "tty" | undefined,
	stderr: "pipe" | "tty" | undefined,
): Promise<void> {
	let streams: Array<tg.Process.Stdio.Stream> = [];
	if (stdout !== undefined) {
		streams.push("stdout");
	}
	if (stderr !== undefined) {
		streams.push("stderr");
	}
	if (streams.length === 0) {
		return;
	}
	let iterator = await tg.client.tryReadProcessStdio(id, {
		...(location !== undefined ? { location } : {}),
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
	location: tg.Location.Arg | undefined,
	signalListener: tg.Host.SignalListener,
): Promise<void> {
	for await (let _ of signalListener) {
		let size = tg.host.getTtySize();
		if (size === null) {
			continue;
		}
		let arg: tg.Process.Tty.Put.Arg = { size };
		if (location !== undefined) {
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
