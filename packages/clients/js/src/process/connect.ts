import * as tg from "../index.ts";
import { capacity, maxChunks } from "./stdio/flow.ts";
import * as spawning from "./spawn.ts";
import { type Connect, connectProcess } from "../client/process/connect.ts";
import { readProcessStdioAll } from "../client/process/stdio/read.ts";
import { writeProcessStdioAll } from "../client/process/stdio/write.ts";
import { Channel } from "./connect/channel.ts";

export class Connection {
	#closed = false;
	#error: unknown;
	// Reserve room for outstanding requests, response acknowledgments, and batched read progress.
	#input = new Channel<Connect.ClientMessage>(maxChunks * 6 + 4);
	#initial: Array<{ arg: tg.Process.Stdio.Read.Arg; id: number }> = [];
	#nextId = 1;
	#reads = new Map<number, Channel<tg.Process.Stdio.Read.ServerMessage>>();
	#requests = new Map<
		number,
		{
			reject: (error: unknown) => void;
			resolve: (output: Connect.ServerResponseOutput) => void;
		}
	>();
	#wait = Promise.withResolvers<tg.Process.Wait | null>();
	#waited = false;

	private constructor() {
		this.#wait.promise.catch(() => {});
	}

	static async open(
		arg: Connect.Arg,
	): Promise<{ connection: Connection; output: tg.Process.Spawn.Output }> {
		let connection = new Connection();
		for (let [key, read] of Object.entries(arg.reads)) {
			let id = Number(key);
			connection.#initial.push({ arg: read, id });
			connection.#reads.set(id, new Channel(capacity));
			connection.#nextId = Math.max(connection.#nextId, id + 1);
		}
		let initial = connection.#request({ kind: "connect", value: arg }, 0);
		initial.catch(() => {});
		try {
			let output = await connectProcess(tg.client, connection.#input);
			connection.#receive(output).catch((error) => connection.#finish(error));
			let response = await initial;
			if (response.kind !== "connect") {
				throw new Error("expected a connect response");
			}
			if (arg.target.kind === "spawn" && arg.target.value.mode === "spawn") {
				connection.close();
			}
			return { connection, output: response.value };
		} catch (error) {
			connection.#finish(error);
			throw error;
		}
	}

	async #receive(
		output: AsyncIterableIterator<Connect.ServerMessage>,
	): Promise<void> {
		for await (let message of output) {
			if (message.kind === "ack") {
				continue;
			}
			if (message.kind === "response") {
				let response = message.value;
				let read = this.#reads.get(response.id);
				if (read !== undefined) {
					if (response.error !== null) {
						read.close(tg.Error.fromData(response.error));
						this.#reads.delete(response.id);
						this.#input.push({ kind: "ack", value: { id: response.id } }, true);
					} else if (response.output?.kind === "read") {
						read.push({ kind: "response", value: response.output.value });
					} else {
						throw new Error("expected a process read response");
					}
					continue;
				}
				// Reserve a separate queue for acknowledgments so requests cannot block them.
				this.#input.push({ kind: "ack", value: { id: response.id } }, true);
				let pending = this.#requests.get(response.id);
				this.#requests.delete(response.id);
				if (pending === undefined) {
					continue;
				}
				if (response.error !== null) {
					pending.reject(tg.Error.fromData(response.error));
				} else if (response.output !== null) {
					pending.resolve(response.output);
				} else {
					pending.reject(new Error("invalid process response"));
				}
				continue;
			}
			let notification = message.value;
			switch (notification.kind) {
				case "progress":
					break;
				case "read":
					this.#reads
						.get(notification.value.id)
						?.push({ kind: "notification", value: notification.value.event });
					break;
				case "wait":
					this.#waited = true;
					this.#wait.resolve(tg.Process.Wait.fromData(notification.value));
					break;
			}
		}
		this.#finish();
	}

	#request(
		arg: Connect.ClientRequestArg,
		id = this.#nextId++,
	): Promise<Connect.ServerResponseOutput> {
		if (this.#closed) {
			return Promise.reject(
				this.#error ?? new Error("the process connection closed"),
			);
		}
		if (this.#requests.size >= 128 && arg.kind !== "detach") {
			return Promise.reject(new Error("too many process requests"));
		}
		let pending = Promise.withResolvers<Connect.ServerResponseOutput>();
		this.#requests.set(id, pending);
		try {
			if (!this.#input.push({ kind: "request", value: { arg, id } })) {
				throw new Error("the process connection closed");
			}
		} catch (error) {
			this.#requests.delete(id);
			pending.reject(error);
		}
		return pending.promise;
	}

	async wait(): Promise<tg.Process.Wait> {
		let output = await this.#wait.promise;
		if (output === null) {
			throw new Error("the process connection closed before completion");
		}
		return output;
	}

	close(): void {
		this.#finish();
	}

	async detach(): Promise<void> {
		if (!this.#waited) {
			let output;
			try {
				output = await this.#request({ kind: "detach" });
			} catch (error) {
				if (this.#waited) {
					return;
				}
				throw error;
			}
			if (output.kind !== "detach") {
				throw new Error("expected a detach response");
			}
		}
		this.#input.close();
	}

	async signal(arg: tg.Signal.Arg): Promise<void> {
		let output = await this.#request({ kind: "signal", value: arg });
		if (output.kind !== "signal") {
			throw new Error("expected a signal response");
		}
	}

	async cancel(arg: tg.Process.Cancel.Arg): Promise<void> {
		let output = await this.#request({ kind: "cancel", value: arg });
		if (output.kind !== "cancel") {
			throw new Error("expected a cancel response");
		}
	}

	async tty(arg: tg.Process.Tty.Put.Arg): Promise<void> {
		let output = await this.#request({ kind: "tty", value: arg });
		if (output.kind !== "tty") {
			throw new Error("expected a tty response");
		}
	}

	async read(
		id: tg.Process.Id,
		arg: tg.Process.Stdio.Read.Arg,
	): Promise<AsyncIterableIterator<tg.Process.Stdio.Chunk>> {
		let index = this.#initial.findIndex(
			(initial) =>
				initial.arg.streams.length === arg.streams.length &&
				initial.arg.streams.every(
					(stream, index) => stream === arg.streams[index],
				) &&
				(initial.arg.position ?? null) === (arg.position ?? null) &&
				(initial.arg.length ?? null) === (arg.length ?? null) &&
				(initial.arg.size ?? null) === (arg.size ?? null) &&
				(initial.arg.timeout ?? null) === (arg.timeout ?? null),
		);
		let requestId: number;
		let output: Channel<tg.Process.Stdio.Read.ServerMessage>;
		if (index !== -1) {
			requestId = this.#initial.splice(index, 1)[0]!.id;
			output = this.#reads.get(requestId)!;
		} else {
			requestId = this.#nextId++;
			output = new Channel(capacity);
			this.#reads.set(requestId, output);
			try {
				this.#input.push({
					kind: "request",
					value: { arg: { kind: "read", value: arg }, id: requestId },
				});
			} catch (error) {
				this.#reads.delete(requestId);
				throw error;
			}
		}
		let ended = false;
		let connection = {
			input: {
				push: (message: tg.Process.Stdio.Read.ClientMessage): boolean => {
					if (message.kind === "ack") {
						ended = true;
						this.#reads.delete(requestId);
						return this.#input.push(
							{ kind: "ack", value: { id: requestId } },
							true,
						);
					}
					return this.#input.push(
						{
							kind: "notification",
							value: {
								kind: "read",
								value: { id: requestId, progress: message.value },
							},
						},
						true,
					);
				},
				close: () => {
					this.#reads.delete(requestId);
					if (!ended) {
						this.#close(requestId);
					}
				},
			},
			output,
			reconnect: false,
		};
		return readProcessStdioAll(tg.client, id, arg, connection);
	}

	async write(
		id: tg.Process.Id,
		arg: tg.Process.Stdio.Write.Stream.Arg,
		input: AsyncIterableIterator<tg.Process.Stdio.Chunk>,
		complete?: (chunk: tg.Process.Stdio.Chunk) => void,
	): Promise<void> {
		let output = new Channel<tg.Process.Stdio.Write.ServerMessage>(capacity);
		let previous = Promise.resolve();
		let connection = {
			input: {
				push: (message: tg.Process.Stdio.Write.ClientMessage): boolean => {
					if (message.kind === "ack") {
						return true;
					}
					let request = message.value;
					let task = this.#request({
						kind: "write",
						value: {
							data: request.arg,
							...(arg.location !== undefined ? { location: arg.location } : {}),
							...(arg.tokens !== undefined ? { tokens: arg.tokens } : {}),
						},
					}).then((response) => {
						if (response.kind !== "write") {
							throw new Error("expected a write response");
						}
						return {
							kind: "response",
							value: { error: null, id: request.id, output: response.value },
						} as tg.Process.Stdio.Write.ServerMessage;
					});
					// Requests travel immediately; expose their outcomes in input order.
					task.catch(() => {});
					let ordered = previous.then(async () => {
						output.push(await task);
					});
					ordered.catch((error) => output.close(error));
					previous = ordered;
					return true;
				},
				close: () => output.close(),
			},
			output,
			reconnect: false,
		};
		await writeProcessStdioAll(tg.client, id, arg, input, connection, complete);
	}

	#close(id: number): void {
		try {
			this.#input.push({
				kind: "request",
				value: { arg: { kind: "close", value: id }, id: this.#nextId++ },
			});
		} catch (error) {
			this.#finish(error);
		}
	}

	stdioClient(): Pick<
		typeof tg.client,
		"tryReadProcessStdio" | "writeProcessStdio" | "setProcessTtySize"
	> {
		return {
			setProcessTtySize: (_id, arg) => this.tty(arg),
			tryReadProcessStdio: (id, arg) => this.read(id, arg),
			writeProcessStdio: (id, arg, input, complete) =>
				this.write(id, arg, input, complete),
		};
	}

	#finish(error?: unknown): void {
		if (this.#closed) {
			return;
		}
		this.#closed = true;
		this.#error = error;
		this.#input.close();
		for (let request of this.#requests.values()) {
			request.reject(error ?? new Error("the process connection closed"));
		}
		this.#requests.clear();
		for (let read of this.#reads.values()) {
			read.close(error);
		}

		if (error === undefined) {
			this.#wait.resolve(null);
		} else {
			this.#wait.reject(error);
		}
	}
}

export async function connect<O extends tg.Value>(
	id: tg.Process.Id,
	options: Connect.Options = {},
): Promise<tg.Process<O>> {
	let reads = Object.fromEntries(
		(options.reads ?? []).map((arg, index) => [index + 1, arg]),
	);
	let { connection, output } = await Connection.open({
		reads,
		target: {
			kind: "existing",
			value: {
				id,
				lease: options.lease ?? null,
				location: options.location ?? null,
				tokens: options.tokens ?? {},
			},
		},
	});
	return new tg.Process<O>({
		connection,
		id,
		lease: output.lease ?? null,
		location:
			output.location === undefined || output.location === null
				? null
				: tg.Location.Arg.fromLocation(output.location),
		stderr: new tg.Process.Stdio.Reader({ stream: "stderr" }),
		stdin: new tg.Process.Stdio.Writer({ stream: "stdin" }),
		stdout: new tg.Process.Stdio.Reader({ stream: "stdout" }),
		tokens: output.tokens ?? {},
	});
}

export async function spawn<O extends tg.Value>(
	arg: tg.Process.Spawn.Arg,
	options: tg.Referent.Options,
	mode: Connect.Mode,
): Promise<tg.Process<O>> {
	return arg.sandbox === undefined
		? spawning.spawnUnsandboxed<O>(arg, options)
		: spawning.spawnSandboxed<O>(arg, options, mode);
}
