import * as tg from "../../index.ts";
import { capacity, maxChunks } from "../stdio/flow.ts";
import {
	type Connect,
	connectProcess,
	requestWindow,
} from "../../client/process/connect.ts";
import type { Connection as ReadConnection } from "../../client/process/stdio/read.ts";
import type { Connection as WriteConnection } from "../../client/process/stdio/write.ts";
import { Channel } from "./channel.ts";

export class Session {
	#closed = false;
	#confirmed = false;
	#credit = Promise.withResolvers<void>();
	#error: unknown;
	// Reserve room for outstanding requests, response acknowledgments, and batched read progress.
	#input = new Channel<Connect.ClientMessage>(maxChunks * 6 + 4);
	#initial: Array<{
		arg: tg.Process.Stdio.Read.Arg;
		id: number;
		output: Channel<tg.Process.Stdio.Read.ServerMessage>;
	}> = [];
	#nextId = 1;
	output: tg.Process.Spawn.Output | null = null;
	#reads = new Map<number, Channel<tg.Process.Stdio.Read.ServerMessage>>();
	#requests = new Map<
		number,
		{
			reject: (error: unknown) => void;
			resolve: (output: Connect.ServerResponseOutput) => void;
		}
	>();
	#unacknowledged = new Set<number>();
	#wait = Promise.withResolvers<tg.Process.Wait | null>();
	#waited = false;

	private constructor() {
		this.#wait.promise.catch(() => {});
	}

	static async open(
		arg: Connect.Arg,
	): Promise<{ connection: Session; output: tg.Process.Spawn.Output }> {
		let connection = new Session();
		for (let [key, read] of Object.entries(arg.reads)) {
			let id = Number(key);
			let output = new Channel<tg.Process.Stdio.Read.ServerMessage>(capacity);
			connection.#initial.push({ arg: read, id, output });
			connection.#reads.set(id, output);
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
			connection.output = response.value;
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
				this.#unacknowledged.delete(message.value.id);
				this.#credit.resolve();
				this.#credit = Promise.withResolvers<void>();
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
				if (response.id !== 0)
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

	async #request(
		arg: Connect.ClientRequestArg,
		id = this.#nextId++,
	): Promise<Connect.ServerResponseOutput> {
		if (this.#closed) {
			return Promise.reject(
				this.#error ?? new Error("the process connection closed"),
			);
		}
		if (this.#requests.size >= requestWindow && arg.kind !== "detach") {
			return Promise.reject(new Error("too many process requests"));
		}
		let pending = Promise.withResolvers<Connect.ServerResponseOutput>();
		this.#requests.set(id, pending);
		try {
			await this.#sendRequest(arg, id);
			if (arg.kind !== "connect") this.confirm();
		} catch (error) {
			this.#requests.delete(id);
			pending.reject(error);
		}
		return pending.promise;
	}

	async #sendRequest(arg: Connect.ClientRequestArg, id: number): Promise<void> {
		if (id !== 0) {
			const limit = requestWindow + Number(arg.kind === "detach");
			while (this.#unacknowledged.size >= limit && !this.#closed) {
				await this.#credit.promise;
			}
			if (this.#closed) {
				throw this.#error ?? new Error("the process connection closed");
			}
			this.#unacknowledged.add(id);
		}
		try {
			if (!this.#input.push({ kind: "request", value: { arg, id } })) {
				throw new Error("the process connection closed");
			}
		} catch (error) {
			this.#unacknowledged.delete(id);
			throw error;
		}
	}

	get closed(): boolean {
		return this.#closed;
	}

	confirm(): void {
		if (this.#confirmed) return;
		this.#confirmed = true;
		// Keep the opening acknowledgment behind the operation that caused a reconnect.
		this.#input.push({ kind: "ack", value: { id: 0 } });
	}

	hasInitial(arg: tg.Process.Stdio.Read.Arg): boolean {
		return this.#initial.some((initial) => matchesRead(initial.arg, arg));
	}

	closeInitial(stream: tg.Process.Stdio.Stream): void {
		let index = this.#initial.findIndex((initial) =>
			matchesRead(initial.arg, { streams: [stream] }),
		);
		if (index === -1) return;
		let { id, output } = this.#initial.splice(index, 1)[0]!;
		output.close();
		this.#reads.delete(id);
		this.#close(id);
	}

	async wait(): Promise<tg.Process.Wait> {
		this.confirm();
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

	async read(arg: tg.Process.Stdio.Read.Arg): Promise<ReadConnection> {
		let index = this.#initial.findIndex((initial) =>
			matchesRead(initial.arg, arg),
		);
		let requestId: number;
		let output: Channel<tg.Process.Stdio.Read.ServerMessage>;
		if (index !== -1) {
			let initial = this.#initial.splice(index, 1)[0]!;
			requestId = initial.id;
			output = initial.output;
		} else {
			if (this.#closed)
				throw this.#error ?? new Error("the process connection closed");
			requestId = this.#nextId++;
			output = new Channel(capacity);
			this.#reads.set(requestId, output);
			try {
				await this.#sendRequest({ kind: "read", value: arg }, requestId);
			} catch (error) {
				this.#reads.delete(requestId);
				throw error;
			}
		}
		this.confirm();
		let ended = false;
		let closed = false;
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
					if (closed) return;
					closed = true;
					output.close();
					this.#reads.delete(requestId);
					if (!ended) {
						this.#close(requestId);
					}
				},
			},
			output,
		};
		return connection;
	}

	write(arg: tg.Process.Stdio.Write.Stream.Arg): WriteConnection {
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
		};
		return connection;
	}

	#close(id: number): void {
		if (this.#closed) return;
		this.#sendRequest({ kind: "close", value: id }, this.#nextId++).catch(
			(error) => this.#finish(error),
		);
	}

	#finish(error?: unknown): void {
		if (this.#closed) {
			return;
		}
		this.#closed = true;
		this.#error = error;
		this.#credit.resolve();
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

function matchesRead(
	initial: tg.Process.Stdio.Read.Arg,
	arg: tg.Process.Stdio.Read.Arg,
): boolean {
	return (
		initial.streams.length === arg.streams.length &&
		initial.streams.every((stream, index) => stream === arg.streams[index]) &&
		(initial.position ?? 0) === (arg.position ?? 0) &&
		(initial.length ?? null) === (arg.length ?? null) &&
		(initial.size ?? null) === (arg.size ?? null) &&
		(initial.timeout ?? null) === (arg.timeout ?? null)
	);
}
