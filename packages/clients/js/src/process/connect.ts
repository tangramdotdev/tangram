import * as tg from "../index.ts";
import * as spawning from "./spawn.ts";
import { type Connect, connectProcess } from "../client/process/connect.ts";
import { readProcessStdioAll } from "../client/process/stdio/read.ts";
import { writeProcessStdioAll } from "../client/process/stdio/write.ts";
import { Channel } from "./connect/channel.ts";

export class Connection {
	#closed = false;
	#error: unknown;
	#input = new Channel<Connect.ClientMessage>(64);
	#initial: Array<{ arg: tg.Process.Stdio.Read.Arg; id: number }> = [];
	#nextId = 1;
	#reads = new Map<number, Channel<tg.Process.Stdio.Read.ServerMessage>>();
	#requests = new Map<
		number,
		{
			resolve: (output: Connect.ServerResponseOutput) => void;
			reject: (error: unknown) => void;
		}
	>();
	#wait = Promise.withResolvers<tg.Process.Wait | null>();
	#waited = false;
	#writes = new Map<number, Channel<tg.Process.Stdio.Write.ServerMessage>>();

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
			connection.#reads.set(id, new Channel(4));
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
				this.#input.push({ kind: "ack", value: { id: response.id } });
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
				case "error": {
					let error = tg.Error.fromData(notification.value.error);
					this.#reads.get(notification.value.id)?.close(error);
					this.#writes.get(notification.value.id)?.close(error);
					break;
				}
				case "progress":
					break;
				case "read":
					this.#reads
						.get(notification.value.id)
						?.push(notification.value.message);
					break;
				case "wait":
					this.#waited = true;
					this.#wait.resolve(tg.Process.Wait.fromData(notification.value));
					break;
				case "write": {
					this.#writes
						.get(notification.value.id)
						?.push(notification.value.message);
					let message = notification.value.message;
					if (
						message.kind === "response" &&
						(message.value.kind === "end" || message.value.value.closed)
					) {
						this.#writes.delete(notification.value.id);
					}
					break;
				}
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
		if (this.#requests.size >= 64 && arg.kind !== "detach") {
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
				JSON.stringify(initial.arg.streams) === JSON.stringify(arg.streams) &&
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
			output = new Channel(4);
			this.#reads.set(requestId, output);
			try {
				await this.#request({ kind: "read", value: arg }, requestId);
			} catch (error) {
				this.#reads.delete(requestId);
				throw error;
			}
		}
		let ended = false;
		let connection = {
			input: {
				push: (message: tg.Process.Stdio.Read.ClientMessage): boolean => {
					if (message.kind === "response") {
						ended = true;
					}
					return this.#input.push({
						kind: "notification",
						value: { kind: "read", value: { id: requestId, message } },
					});
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
		arg: tg.Process.Stdio.Write.Arg,
		input: AsyncIterableIterator<tg.Process.Stdio.Chunk>,
	): Promise<void> {
		let requestId = this.#nextId++;
		let output = new Channel<tg.Process.Stdio.Write.ServerMessage>(4);
		this.#writes.set(requestId, output);
		try {
			await this.#request({ kind: "write", value: arg }, requestId);
		} catch (error) {
			this.#writes.delete(requestId);
			throw error;
		}
		let connection = {
			input: {
				push: (message: tg.Process.Stdio.Write.ClientMessage) =>
					this.#input.push({
						kind: "notification",
						value: { kind: "write", value: { id: requestId, message } },
					}),
				close: () => {
					if (this.#writes.delete(requestId)) {
						this.#close(requestId);
					}
				},
			},
			output,
			reconnect: false,
		};
		await writeProcessStdioAll(tg.client, id, arg, input, connection);
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
			tryReadProcessStdio: (id, arg) => this.read(id, arg),
			writeProcessStdio: (id, arg, input) => this.write(id, arg, input),
			setProcessTtySize: (_id, arg) => this.tty(arg),
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
		for (let write of this.#writes.values()) {
			write.close(error);
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
		location: options.location ?? null,
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
