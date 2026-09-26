import * as tg from "../index.ts";
import * as spawning from "./spawn.ts";
import type { Connect } from "../client/process/connect.ts";
import { readProcessStdioAll } from "../client/process/stdio/read.ts";
import { writeProcessStdioAll } from "../client/process/stdio/write.ts";
import { Session } from "./connect/session.ts";
import type { Connection as ReadConnection } from "../client/process/stdio/read.ts";
import type { Connection as WriteConnection } from "../client/process/stdio/write.ts";

export class Connection {
	#closed = false;
	#initial: Session;
	#opening: Promise<Session> | null = null;
	#session: Session;

	private constructor(session: Session) {
		this.#initial = session;
		this.#session = session;
	}

	static async open(
		arg: Connect.Arg,
	): Promise<{ connection: Connection; output: tg.Process.Spawn.Output }> {
		let { connection: session, output } = await Session.open(arg);
		session.confirm();
		let connection = new Connection(session);
		if (arg.mode === "spawn") connection.close();
		return { connection, output };
	}

	async #ensureSession(read?: tg.Process.Stdio.Read.Arg): Promise<Session> {
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
				reads: read === undefined ? {} : { 1: read },
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

	async wait(): Promise<tg.Process.Wait> {
		while (true) {
			let session = this.#session;
			try {
				return await session.wait();
			} catch (error) {
				if (this.#closed || error instanceof tg.Error) throw error;
				await this.#ensureSession();
			}
		}
	}

	close(): void {
		this.#closed = true;
		this.#session.close();
		this.#initial.close();
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

	async read(
		id: tg.Process.Id,
		arg: tg.Process.Stdio.Read.Arg,
	): Promise<AsyncIterableIterator<tg.Process.Stdio.Chunk>> {
		let connection = await this.#read(arg);
		return readProcessStdioAll(tg.client, id, arg, connection);
	}

	async #read(arg: tg.Process.Stdio.Read.Arg): Promise<ReadConnection> {
		let session = this.#initial.hasInitial(arg)
			? this.#initial
			: await this.#ensureSession(arg);
		let connection = await session.read(arg);
		connection.reconnect = (arg) => this.#read(arg);
		return connection;
	}

	closeInitial(stream: tg.Process.Stdio.Stream): void {
		this.#initial.closeInitial(stream);
	}

	async write(
		id: tg.Process.Id,
		arg: tg.Process.Stdio.Write.Stream.Arg,
		input: AsyncIterableIterator<tg.Process.Stdio.Chunk>,
		complete?: (chunk: tg.Process.Stdio.Chunk) => void,
	): Promise<void> {
		let connection = await this.#write(arg);
		await writeProcessStdioAll(tg.client, id, arg, input, connection, complete);
	}

	async #write(
		arg: tg.Process.Stdio.Write.Stream.Arg,
	): Promise<WriteConnection> {
		let session = await this.#ensureSession();
		let connection = session.write(arg);
		connection.reconnect = () => this.#write(arg);
		return connection;
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
}

export async function connect<O extends tg.Value>(
	id: tg.Process.Id,
	options: Connect.Options = {},
): Promise<tg.Process<O>> {
	let reads = Object.fromEntries(
		(options.reads ?? []).map((arg, index) => [index + 1, arg]),
	);
	let { connection, output } = await Connection.open({
		lease: options.lease ?? null,
		location: options.location ?? null,
		mode: "run",
		process: id,
		reads,
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
