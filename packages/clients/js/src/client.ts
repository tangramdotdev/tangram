import * as tg from "./index.ts";
import { Request, Response } from "./http.ts";
import { checkin } from "./client/checkin.ts";
import { checkout } from "./client/checkout.ts";
import { postObjectBatch } from "./client/object/batch.ts";
import { getObject, tryGetObject } from "./client/object/get.ts";
import { putObject } from "./client/object/put.ts";
import { getProcess, tryGetProcess } from "./client/process/get.ts";
import { putProcess } from "./client/process/put.ts";
import {
	setProcessTtySize,
	trySetProcessTtySize,
} from "./client/process/tty/put.ts";
import { signalProcess, trySignalProcess } from "./client/process/signal.ts";
import { spawnProcess, trySpawnProcess } from "./client/process/spawn.ts";
import { tryReadProcessStdio } from "./client/process/stdio/read.ts";
import {
	tryWriteProcessStdio,
	writeProcessStdio,
} from "./client/process/stdio/write.ts";
import {
	tryWaitProcessPromise,
	waitProcess,
	waitProcessPromise,
} from "./client/process/wait.ts";
import { read, tryRead, tryReadStream } from "./client/read.ts";
import { getSandbox, tryGetSandbox } from "./client/sandbox/get.ts";
import { write } from "./client/write.ts";

type RetryOptions = {
	backoff: number;
	jitter: number;
	maxDelay: number;
	maxRetries: number;
};

let reconnectOptions = defaultRetryOptions();
let retryOptions = defaultRetryOptions();

class RequestError {
	constructor(readonly source: unknown) {}
}

export class Client {
	#connecting: Promise<tg.Host.Http2.ClientHttp2Session> | null = null;
	#session: tg.Host.Http2.ClientHttp2Session | null = null;

	arg() {
		let process = tg.process.env.TANGRAM_PROCESS;
		let token = tg.process.env.TANGRAM_TOKEN;
		let url = tg.process.env.TANGRAM_URL;
		return {
			...(typeof process === "string" ? { process } : {}),
			...(typeof token === "string" ? { token } : {}),
			...(typeof url === "string" ? { url } : {}),
		};
	}

	checkin(
		arg: tg.Checkin.Arg,
	): Promise<AsyncIterableIterator<tg.Progress.Event<tg.Checkin.Output>>> {
		return checkin(this, arg);
	}

	checkout(
		arg: tg.Checkout.Arg,
	): Promise<AsyncIterableIterator<tg.Progress.Event<tg.Checkout.Output>>> {
		return checkout(this, arg);
	}

	getObject(id: tg.Object.Id): Promise<tg.Object.Data> {
		return getObject(this, id);
	}

	tryGetObject(id: tg.Object.Id): Promise<tg.Object.Data | null> {
		return tryGetObject(this, id);
	}

	putObject(
		id: tg.Object.Id,
		arg: tg.Object.Put.Arg,
	): Promise<tg.Object.Put.Output> {
		return putObject(this, id, arg);
	}

	postObjectBatch(arg: tg.Object.Batch.Arg): Promise<tg.Object.Batch.Output> {
		return postObjectBatch(this, arg);
	}

	getProcess(
		id: tg.Process.Id,
		arg?: tg.Process.Get.Arg | null,
	): Promise<tg.Process.Get.Output> {
		return getProcess(this, id, arg);
	}

	tryGetProcess(
		id: tg.Process.Id,
		arg?: tg.Process.Get.Arg | null,
	): Promise<tg.Process.Get.Output | null> {
		return tryGetProcess(this, id, arg);
	}

	putProcess(
		id: tg.Process.Id,
		arg: tg.Process.Put.Arg,
	): Promise<tg.Process.Put.Output> {
		return putProcess(this, id, arg);
	}

	tryReadProcessStdio(
		id: tg.Process.Id,
		arg: tg.Process.Stdio.Read.Arg,
	): Promise<AsyncIterableIterator<tg.Process.Stdio.Read.Event> | null> {
		return tryReadProcessStdio(this, id, arg);
	}

	setProcessTtySize(
		id: tg.Process.Id,
		arg: tg.Process.Tty.Put.Arg,
	): Promise<void> {
		return setProcessTtySize(this, id, arg);
	}

	trySetProcessTtySize(
		id: tg.Process.Id,
		arg: tg.Process.Tty.Put.Arg,
	): Promise<true | null> {
		return trySetProcessTtySize(this, id, arg);
	}

	signalProcess(id: tg.Process.Id, arg: tg.Signal.Arg): Promise<void> {
		return signalProcess(this, id, arg);
	}

	trySignalProcess(
		id: tg.Process.Id,
		arg: tg.Signal.Arg,
	): Promise<true | null> {
		return trySignalProcess(this, id, arg);
	}

	spawnProcess(
		arg: tg.Process.Spawn.Arg,
	): Promise<
		AsyncIterableIterator<tg.Progress.Event<tg.Process.Spawn.Output>>
	> {
		return spawnProcess(this, arg);
	}

	trySpawnProcess(
		arg: tg.Process.Spawn.Arg,
	): Promise<
		AsyncIterableIterator<tg.Progress.Event<tg.Process.Spawn.Output | null>>
	> {
		return trySpawnProcess(this, arg);
	}

	waitProcess(
		id: tg.Process.Id,
		arg: tg.Process.Wait.Arg,
	): Promise<tg.Process.Wait> {
		return waitProcess(this, id, arg);
	}

	waitProcessPromise(
		id: tg.Process.Id,
		arg: tg.Process.Wait.Arg,
	): Promise<() => Promise<tg.Process.Wait | null>> {
		return waitProcessPromise(this, id, arg);
	}

	tryWaitProcessPromise(
		id: tg.Process.Id,
		arg: tg.Process.Wait.Arg,
	): Promise<(() => Promise<tg.Process.Wait | null>) | null> {
		return tryWaitProcessPromise(this, id, arg);
	}

	writeProcessStdio(
		id: tg.Process.Id,
		arg: tg.Process.Stdio.Write.Arg,
		input: AsyncIterableIterator<tg.Process.Stdio.Read.Event>,
	): Promise<AsyncIterableIterator<tg.Process.Stdio.Write.Event>> {
		return writeProcessStdio(this, id, arg, input);
	}

	tryWriteProcessStdio(
		id: tg.Process.Id,
		arg: tg.Process.Stdio.Write.Arg,
		input: AsyncIterableIterator<tg.Process.Stdio.Read.Event>,
	): Promise<AsyncIterableIterator<tg.Process.Stdio.Write.Event> | null> {
		return tryWriteProcessStdio(this, id, arg, input);
	}

	getSandbox(
		id: tg.Sandbox.Id,
		arg?: tg.Sandbox.Get.Arg | null,
	): Promise<tg.Sandbox.Get.Output> {
		return getSandbox(this, id, arg);
	}

	tryGetSandbox(
		id: tg.Sandbox.Id,
		arg?: tg.Sandbox.Get.Arg | null,
	): Promise<tg.Sandbox.Get.Output | null> {
		return tryGetSandbox(this, id, arg);
	}

	objectId(object: tg.Object.Data) {
		return tg.host.objectId(object);
	}

	parseValue(value: string) {
		return tg.host.parseValue(value);
	}

	read(arg: tg.Read.Arg): Promise<Uint8Array> {
		return read(this, arg);
	}

	tryRead(arg: tg.Read.Arg): Promise<Uint8Array | null> {
		return tryRead(this, arg);
	}

	tryReadStream(
		arg: tg.Read.Arg,
	): Promise<AsyncIterableIterator<tg.Read.Event> | null> {
		return tryReadStream(this, arg);
	}

	stringifyValue(value: tg.Value.Data) {
		return tg.host.stringifyValue(value);
	}

	write(
		arg: tg.Write.Arg,
		input: AsyncIterableIterator<Uint8Array>,
	): Promise<tg.Write.Output>;
	write(bytes: string | Uint8Array): Promise<tg.Blob.Id>;
	write(
		argOrBytes: tg.Write.Arg | string | Uint8Array,
		input?: AsyncIterableIterator<Uint8Array>,
	): Promise<tg.Write.Output | tg.Blob.Id> {
		return write(this, argOrBytes, input);
	}

	async send(request: Request): Promise<Response> {
		try {
			return await this.#send(request);
		} catch (error) {
			throw error instanceof RequestError ? error.source : error;
		}
	}

	async sendWithRetry(request: Request): Promise<Response> {
		if (request.body !== undefined && !request.body.replayable) {
			throw new Error("cannot retry a request with a streaming body");
		}
		try {
			return await retry(
				retryOptions,
				() => this.#send(request),
				(error) => error instanceof RequestError,
			);
		} catch (error) {
			throw error instanceof RequestError ? error.source : error;
		}
	}

	async #send(request: Request): Promise<Response> {
		let token = tg.process.env.TANGRAM_TOKEN;
		if (token !== undefined && typeof token !== "string") {
			throw new Error("invalid TANGRAM_TOKEN");
		}
		let headers: tg.Host.Http2.Headers = {
			...request.headers.toData(),
			":method": request.method,
			":path": request.uri.toString(),
		};
		if (token !== undefined) {
			headers = {
				...headers,
				authorization: `Bearer ${token}`,
			};
		}
		let session = await this.#connect();
		try {
			let body = request.body;
			let stream = session.request(headers, {
				endStream: body === undefined,
			});
			let response = Response.fromStream(stream);
			if (body !== undefined) {
				(async () => {
					for await (let chunk of body) {
						stream.write(chunk);
					}
					stream.end();
				})().catch((error) => {
					stream.destroy(
						error instanceof Error
							? error
							: new Error("failed to write the request body"),
					);
				});
			}
			return await response;
		} catch (error) {
			this.#disconnect(session);
			session.destroy();
			throw new RequestError(error);
		}
	}

	async #connect() {
		while (true) {
			let session = this.#session;
			if (session !== null) {
				if (!session.closed) {
					return session;
				}
				this.#disconnect(session);
				session.destroy();
			}
			let connecting = this.#connecting;
			if (connecting === null) {
				connecting = retry(reconnectOptions, () => this.#createSession());
				this.#connecting = connecting;
			}
			let nextSession: tg.Host.Http2.ClientHttp2Session;
			try {
				nextSession = await connecting;
			} finally {
				if (this.#connecting === connecting) {
					this.#connecting = null;
				}
			}
			if (nextSession.closed) {
				nextSession.destroy();
				continue;
			}
			if (this.#session === null) {
				this.#session = nextSession;
				nextSession.once("close", () => this.#disconnect(nextSession));
				nextSession.once("error", () => this.#disconnect(nextSession));
			} else if (this.#session !== nextSession) {
				nextSession.destroy();
			}
			return this.#session;
		}
	}

	async #createSession() {
		let url = tg.process.env.TANGRAM_URL;
		if (typeof url !== "string") {
			throw new Error("missing TANGRAM_URL");
		}
		let session = tg.host.http2.connect(url);
		try {
			await new Promise<void>((resolve, reject) => {
				let cleanup = () => {
					session.off("connect", onConnect);
					session.off("error", onError);
				};
				let onConnect = () => {
					cleanup();
					resolve();
				};
				let onError = (error: unknown) => {
					cleanup();
					reject(error);
				};
				session.once("connect", onConnect);
				session.once("error", onError);
			});
		} catch (error) {
			session.destroy();
			throw error;
		}
		return session;
	}

	#disconnect(value?: tg.Host.Http2.ClientHttp2Session) {
		if (value !== undefined && this.#session !== value) {
			return;
		}
		this.#session = null;
	}
}

export let client = new Client();

function defaultRetryOptions(): RetryOptions {
	return {
		backoff: 0.01,
		jitter: 0.01,
		maxDelay: 1,
		maxRetries: 3,
	};
}

async function retry<T>(
	options: RetryOptions,
	function_: () => Promise<T>,
	shouldRetry: (error: unknown) => boolean = () => true,
) {
	let error: unknown;
	for (let attempt = 0; attempt <= options.maxRetries; attempt++) {
		try {
			return await function_();
		} catch (error_) {
			error = error_;
			if (attempt === options.maxRetries || !shouldRetry(error)) {
				break;
			}
			let multiplier = 2 ** Math.min(attempt + 1, 31);
			let jitter = Math.random() * options.jitter;
			let delay = Math.min(
				options.backoff * multiplier + jitter,
				options.maxDelay,
			);
			await tg.sleep(delay);
		}
	}
	throw error;
}
