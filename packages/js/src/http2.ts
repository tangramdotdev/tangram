type Headers = Record<string, string | number | string[] | undefined>;

type ConnectOptions = {
	port?: number | undefined;
};

type RequestOptions = {
	endStream?: boolean | undefined;
};

type StreamEvent =
	| { kind: "close" }
	| { kind: "data"; bytes: Uint8Array }
	| { kind: "end" }
	| { kind: "error"; message: string }
	| { kind: "response"; headers: [string, string][] }
	| { kind: "trailers"; headers: [string, string][] };

class EventEmitter {
	#listeners = new Map<string, Set<(...args: unknown[]) => void>>();

	on(event: string, listener: (...args: unknown[]) => void) {
		let listeners = this.#listeners.get(event);
		if (listeners === undefined) {
			listeners = new Set();
			this.#listeners.set(event, listeners);
		}
		listeners.add(listener);
		return this;
	}

	once(event: string, listener: (...args: unknown[]) => void) {
		let wrapped = (...args: unknown[]) => {
			this.off(event, wrapped);
			listener(...args);
		};
		return this.on(event, wrapped);
	}

	off(event: string, listener: (...args: unknown[]) => void) {
		this.#listeners.get(event)?.delete(listener);
		return this;
	}

	emit(event: string, ...args: unknown[]) {
		let listeners = this.#listeners.get(event);
		if (listeners === undefined) {
			return false;
		}
		for (let listener of Array.from(listeners)) {
			listener(...args);
		}
		return true;
	}
}

export class ClientHttp2Session extends EventEmitter {
	#closed = false;
	#connect: Promise<void>;
	#destroyed = false;
	#token: number | undefined;

	constructor(
		readonly authority: string,
		readonly options: ConnectOptions = {},
	) {
		super();
		this.#connect = syscall("http2_connect", authority, options).then(
			(token) => {
				this.#token = token;
				this.emit("connect", this);
			},
			(error) => {
				this.#closed = true;
				this.#destroyed = true;
				this.emit("error", error);
				this.emit("close");
				throw error;
			},
		);
		this.#connect.catch(() => undefined);
	}

	request(headers: Headers, options: RequestOptions = {}) {
		let stream = new ClientHttp2Stream(this, headers, options);
		return stream;
	}

	async close(callback?: () => void) {
		if (callback !== undefined) {
			this.once("close", callback);
		}
		if (this.#closed) {
			return;
		}
		this.#closed = true;
		try {
			let token = await this.#ready();
			await syscall("http2_session_close", token);
		} finally {
			this.emit("close");
		}
	}

	destroy(error?: Error) {
		if (this.#destroyed) {
			return;
		}
		this.#destroyed = true;
		this.#closed = true;
		this.#connect
			.then(() =>
				this.#token === undefined
					? undefined
					: syscall("http2_session_destroy", this.#token, error?.message),
			)
			.catch(() => undefined)
			.finally(() => {
				if (error !== undefined) {
					this.emit("error", error);
				}
				this.emit("close");
			});
	}

	async _request(headers: Headers, options: RequestOptions) {
		let token = await this.#ready();
		return syscall("http2_session_request", token, headersToEntries(headers), {
			end_stream: options.endStream ?? defaultEndStream(headers),
		});
	}

	async #ready() {
		await this.#connect;
		if (this.#token === undefined) {
			throw new Error("the HTTP/2 session is closed");
		}
		return this.#token;
	}
}

export class ClientHttp2Stream extends EventEmitter {
	#closed = false;
	#encoding: "utf8" | undefined;
	#ready: Promise<number>;
	#token: number | undefined;

	constructor(
		readonly session: ClientHttp2Session,
		headers: Headers,
		options: RequestOptions,
	) {
		super();
		this.#ready = session._request(headers, options).then(
			(token) => {
				this.#token = token;
				this.#readLoop(token);
				return token;
			},
			(error) => {
				this.#closed = true;
				this.emit("error", error);
				this.emit("close");
				throw error;
			},
		);
		this.#ready.catch(() => undefined);
	}

	write(bytes: string | Uint8Array) {
		this.#ready
			.then((token) => syscall("http2_stream_write", token, toBytes(bytes)))
			.catch((error) => this.emit("error", error));
		return true;
	}

	end(bytes?: string | Uint8Array) {
		this.#ready
			.then((token) =>
				syscall(
					"http2_stream_end",
					token,
					bytes === undefined ? undefined : toBytes(bytes),
				),
			)
			.catch((error) => this.emit("error", error));
		return this;
	}

	close() {
		this.#close();
		return this;
	}

	destroy(error?: Error) {
		if (error !== undefined) {
			this.emit("error", error);
		}
		this.#close();
		return this;
	}

	setEncoding(encoding: "utf8" | "utf-8") {
		this.#encoding = "utf8";
		return this;
	}

	async #readLoop(token: number) {
		try {
			while (!this.#closed) {
				let event = (await syscall("http2_stream_read", token)) as
					| StreamEvent
					| undefined;
				if (event === undefined) {
					break;
				}
				if (event.kind === "response") {
					this.emit("response", entriesToHeaders(event.headers));
				} else if (event.kind === "data") {
					this.emit(
						"data",
						this.#encoding === undefined
							? event.bytes
							: syscall("encoding_utf8_decode", event.bytes),
					);
				} else if (event.kind === "trailers") {
					this.emit("trailers", entriesToHeaders(event.headers));
				} else if (event.kind === "end") {
					this.emit("end");
				} else if (event.kind === "error") {
					this.emit("error", new Error(event.message));
				} else if (event.kind === "close") {
					break;
				}
			}
		} catch (error) {
			this.emit("error", error);
		} finally {
			this.#closed = true;
			this.emit("close");
		}
	}

	#close() {
		if (this.#closed) {
			return;
		}
		this.#closed = true;
		this.#ready
			.then((token) => syscall("http2_stream_close", token))
			.catch(() => undefined)
			.finally(() => this.emit("close"));
	}
}

export function connect(
	authority: string | { toString(): string },
	options?: ConnectOptions | ((session: ClientHttp2Session) => void),
	listener?: (session: ClientHttp2Session) => void,
) {
	if (typeof options === "function") {
		listener = options;
		options = undefined;
	}
	let session = new ClientHttp2Session(authority.toString(), options);
	if (listener !== undefined) {
		session.once("connect", listener as (...args: unknown[]) => void);
	}
	return session;
}

export let constants = {
	HTTP2_HEADER_AUTHORITY: ":authority",
	HTTP2_HEADER_METHOD: ":method",
	HTTP2_HEADER_PATH: ":path",
	HTTP2_HEADER_SCHEME: ":scheme",
	HTTP2_HEADER_STATUS: ":status",
	HTTP2_METHOD_GET: "GET",
	HTTP2_METHOD_POST: "POST",
} as const;

export let http2 = {
	ClientHttp2Session,
	ClientHttp2Stream,
	connect,
	constants,
};

function headersToEntries(headers: Headers) {
	let entries: [string, string][] = [];
	for (let [name, value] of Object.entries(headers)) {
		if (value === undefined) {
			continue;
		}
		name = name.toLowerCase();
		if (Array.isArray(value)) {
			for (let item of value) {
				entries.push([name, item]);
			}
		} else {
			entries.push([name, value.toString()]);
		}
	}
	return entries;
}

function entriesToHeaders(entries: [string, string][]) {
	let headers: Record<string, string | string[]> = {};
	for (let [name, value] of entries) {
		let current = headers[name];
		if (current === undefined) {
			headers[name] = value;
		} else if (Array.isArray(current)) {
			current.push(value);
		} else {
			headers[name] = [current, value];
		}
	}
	return headers;
}

function defaultEndStream(headers: Headers) {
	let method = (headers[":method"] ?? "GET").toString().toUpperCase();
	return method !== "POST" && method !== "PUT" && method !== "PATCH";
}

function toBytes(value: string | Uint8Array) {
	return typeof value === "string" ? syscall("encoding_utf8_encode", value) : value;
}
