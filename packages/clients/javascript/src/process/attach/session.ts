import * as tg from "../../index.ts";
import { type Attach, attachProcess } from "../../client/process/attach.ts";
import { Channel } from "./channel.ts";

export class Session {
	#closed = false;
	#ready = false;
	#selected = Promise.withResolvers<tg.Process.Spawn.Output>();
	#error: unknown;
	#input: Channel<Attach.ClientMessage>;
	#nextId = 1;
	#outcome = Promise.withResolvers<tg.Process.Outcome | null>();
	#outcomeReceived = false;
	output: tg.Process.Spawn.Output | null = null;
	#requests = new Map<
		number,
		{
			reject: (error: unknown) => void;
			resolve: (output: Attach.ServerResponseOutput) => void;
		}
	>();
	#pending = new Set<number>();

	private constructor(readonly client: tg.Client) {
		this.#input = new Channel(client.process.attach.maxMessages);
		this.#outcome.promise.catch(() => {});
		this.#selected.promise.catch(() => {});
	}

	static async open(
		arg: Attach.Arg,
		client: tg.Client = tg.client,
	): Promise<{ connection: Session; output: tg.Process.Spawn.Output }> {
		let connection = new Session(client);
		try {
			let [, output] = await attachProcess(client, arg, connection.#input);
			connection.#receive(output).catch((error) => connection.#finish(error));
			connection.output = await connection.#selected.promise;
			return { connection, output: connection.output };
		} catch (error) {
			connection.#finish(error);
			throw error;
		}
	}

	async #receive(
		output: AsyncIterableIterator<Attach.ServerMessage>,
	): Promise<void> {
		for await (let message of output) {
			if (message.kind === "ack") {
				continue;
			}
			if (message.kind === "response") {
				let response = message.value;
				this.#pending.delete(response.id);
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
					if (notification.value.kind === "output")
						this.#selected.resolve(notification.value.value);
					break;
				case "outcome":
					this.#outcomeReceived = true;
					this.#outcome.resolve(
						tg.Process.Outcome.fromData(notification.value),
					);
					break;
			}
		}
		this.#finish();
	}

	async #request(
		arg: Attach.ClientRequestArg,
		id = this.#nextId++,
	): Promise<Attach.ServerResponseOutput> {
		if (this.#closed) {
			return Promise.reject(
				this.#error ?? new Error("the process connection closed"),
			);
		}
		let pending = Promise.withResolvers<Attach.ServerResponseOutput>();
		this.#requests.set(id, pending);
		try {
			await this.#sendRequest(arg, id);
			this.ready();
		} catch (error) {
			this.#requests.delete(id);
			pending.reject(error);
		}
		return pending.promise;
	}

	async #sendRequest(arg: Attach.ClientRequestArg, id: number): Promise<void> {
		if (this.#closed)
			throw this.#error ?? new Error("the process connection closed");
		if (this.#pending.size >= this.client.process.attach.maxRequests) {
			throw new Error("too many pending process requests");
		}
		this.#pending.add(id);
		try {
			if (
				!this.#input.push(
					{ kind: "request", value: { arg, id } },
					arg.kind === "detach",
				)
			) {
				throw new Error("the process connection closed");
			}
		} catch (error) {
			this.#pending.delete(id);
			throw error;
		}
	}

	get closed(): boolean {
		return this.#closed;
	}

	ready(): void {
		if (this.#ready) return;
		this.#ready = true;
		// Keep the ready notification behind the operation that caused a reconnect.
		this.#input.push({ kind: "notification", value: { kind: "ready" } });
	}

	async wait(): Promise<tg.Process.Outcome> {
		this.ready();
		let outcome = await this.#outcome.promise;
		if (outcome === null) {
			throw new Error("the process connection closed before completion");
		}
		return outcome;
	}

	close(): void {
		this.#finish();
	}

	async detach(): Promise<void> {
		if (!this.#outcomeReceived) {
			let output;
			try {
				output = await this.#request({ kind: "detach" });
			} catch (error) {
				if (this.#outcomeReceived) {
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

	#finish(error?: unknown): void {
		this.#selected.reject(
			error ?? new Error("the process connection closed before selection"),
		);
		if (this.#closed) {
			return;
		}
		this.#closed = true;
		this.#error = error;
		this.#pending.clear();
		this.#input.close();
		for (let request of this.#requests.values()) {
			request.reject(error ?? new Error("the process connection closed"));
		}
		this.#requests.clear();

		if (error === undefined) {
			this.#outcome.resolve(null);
		} else {
			this.#outcome.reject(error);
		}
	}
}
