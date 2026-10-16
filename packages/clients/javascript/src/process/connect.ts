import * as tg from "../index.ts";
import * as spawning from "./spawn.ts";
import { isTransportError } from "../client.ts";
import type { Connect } from "../client/process/connect.ts";
import { Session } from "./connect/session.ts";

export class Connection {
	#closed = false;
	#closing = Promise.withResolvers<void>();
	#opening: Promise<Session> | null = null;
	#session: Session;

	private constructor(session: Session) {
		this.#session = session;
	}

	static async open(
		arg: Connect.Arg,
	): Promise<{ connection: Connection; output: tg.Process.Spawn.Output }> {
		let { connection: session, output } = await Session.open(arg);
		session.ready();
		let connection = new Connection(session);
		if (arg.mode === "spawn") connection.close();
		return { connection, output };
	}

	async #ensureSession(): Promise<Session> {
		if (this.#closed) throw new Error("the process connection was closed");
		if (!this.#session.closed) return this.#session;
		if (this.#opening === null) {
			let output = this.#session.output!;
			if (typeof output.process !== "string")
				throw new Error("expected a sandboxed process");
			let arg: Connect.Arg = {
				lease: output.lease ?? null,
				location:
					output.location === undefined || output.location === null
						? null
						: tg.Location.Arg.fromLocation(output.location),
				mode: "run",
				process: output.process,
				tokens: output.tokens ?? {},
			};
			this.#opening = Session.open(arg).then(({ connection: session }) => {
				if (this.#closed) {
					session.close();
					throw new Error("the process connection was closed");
				}
				this.#session = session;
				return session;
			});
		}
		let opening = this.#opening;
		try {
			return await opening;
		} finally {
			if (this.#opening === opening) this.#opening = null;
		}
	}

	async wait(): Promise<tg.Process.Outcome> {
		while (true) {
			let session = this.#session;
			try {
				return await session.wait();
			} catch (error) {
				if (
					this.#closed ||
					(error instanceof tg.Error && !isTransportError(error))
				)
					throw error;
				await this.#ensureSession();
			}
		}
	}

	close(): void {
		this.#closed = true;
		this.#closing.resolve();
		this.#session.close();
	}

	async detach(): Promise<void> {
		if (this.#closed) return;
		if (!this.#session.closed) await this.#session.detach();
		this.close();
	}

	async signal(arg: tg.Signal.Arg): Promise<void> {
		await (await this.#ensureSession()).signal(arg);
	}
	async cancel(arg: tg.Process.Cancel.Arg): Promise<void> {
		await (await this.#ensureSession()).cancel(arg);
	}
	async tty(arg: tg.Process.Tty.Put.Arg): Promise<void> {
		await (await this.#ensureSession()).tty(arg);
	}

	/** Get a stdio client for a process located by its connect's nonce, whose reads end when the connection closes once it opens. */
	static stdioClient(
		connection: Promise<Connection | null>,
	): Pick<
		typeof tg.client,
		"tryReadProcessStdio" | "writeProcessStdio" | "setProcessTtySize"
	> {
		return {
			setProcessTtySize: (process, arg) =>
				tg.client.setProcessTtySize(process, arg),
			tryReadProcessStdio: async (process, arg) => {
				let iterator = await tg.client.tryReadProcessStdio(process, arg);
				let opened = await connection.catch(() => null);
				if (iterator === null || opened === null) return iterator;
				if (opened.#closed) {
					await iterator.return?.();
					return null;
				}
				return opened.#bind(iterator);
			},
			writeProcessStdio: (process, arg, input, complete) =>
				tg.client.writeProcessStdio(process, arg, input, complete),
		};
	}

	stdioClient(): Pick<
		typeof tg.client,
		"tryReadProcessStdio" | "writeProcessStdio" | "setProcessTtySize"
	> {
		return {
			setProcessTtySize: (_id, arg) => this.tty(arg),
			tryReadProcessStdio: async (id, arg) => {
				if (this.#closed) return null;
				let iterator = await tg.client.tryReadProcessStdio(id, arg);
				return iterator === null ? null : this.#bind(iterator);
			},
			writeProcessStdio: (id, arg, input, complete) =>
				tg.client.writeProcessStdio(id, arg, input, complete),
		};
	}

	// Reads bound to the connection end when it closes, so the connection's stdio task finishes.
	#bind(
		iterator: AsyncIterableIterator<tg.Process.Stdio.Chunk>,
	): AsyncIterableIterator<tg.Process.Stdio.Chunk> {
		let closing = this.#closing.promise.then(
			(): IteratorResult<tg.Process.Stdio.Chunk> => ({
				done: true,
				value: undefined,
			}),
		);
		let bound: AsyncIterableIterator<tg.Process.Stdio.Chunk> = {
			next: async () => {
				let result = await Promise.race([iterator.next(), closing]);
				if (result.done) iterator.return?.().catch(() => {});
				return result;
			},
			return: async () => {
				await iterator.return?.();
				return { done: true, value: undefined };
			},
			[Symbol.asyncIterator]: () => bound,
		};
		return bound;
	}
}

export async function connect<O extends tg.Value>(
	id: tg.Process.Id,
	options: Connect.Options = {},
): Promise<tg.Process<O>> {
	let { connection, output } = await Connection.open({
		lease: options.lease ?? null,
		location: options.location ?? null,
		mode: "run",
		process: id,
		tokens: options.tokens ?? {},
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
