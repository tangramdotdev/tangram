import * as tg from "../index.ts";
import { Body } from "./body.ts";
import { requireJson } from "./encoding.ts";
import { Headers } from "./headers.ts";

export class Response {
	body: Body;
	headers: Headers;
	status: number;
	#close: (() => void) | undefined;

	constructor(
		status: number,
		headers: Headers | tg.Host.Http2.Headers,
		body: Body,
		close?: () => void,
	) {
		this.body = body;
		this.#close = close;
		this.headers = headers instanceof Headers ? headers : new Headers(headers);
		this.status = status;
	}

	static fromStream(stream: tg.Host.Http2.ClientHttp2Stream) {
		let chunks: Array<Uint8Array> = [];
		let error: unknown = null;
		let failed = false;
		let notify: (() => void) | undefined;
		let done = false;
		let settled = false;

		let body = new Body({
			async *[Symbol.asyncIterator]() {
				try {
					while (true) {
						if (chunks.length > 0) {
							yield chunks.shift()!;
						} else if (failed) {
							throw error;
						} else if (done) {
							break;
						} else {
							await new Promise<void>((resolve) => {
								notify = resolve;
								stream.resume();
							});
							notify = undefined;
						}
					}
				} finally {
					if (!done) {
						stream.close();
					}
				}
			},
		});

		return new Promise<Response>((resolve, reject) => {
			let fail = (error_: unknown) => {
				error = error_;
				failed = true;
				done = true;
				stream.close();
				notify?.();
				if (!settled) {
					settled = true;
					reject(error_);
				}
			};

			stream.once("error", fail);
			stream.once("response", (headers: unknown) => {
				try {
					let headers_ = new Headers(headers as tg.Host.Http2.Headers);
					let status = Number(headers_.get(":status"));
					if (!Number.isInteger(status)) {
						throw new Error("invalid status");
					}
					settled = true;
					resolve(new Response(status, headers_, body, () => stream.close()));
				} catch (error) {
					fail(error);
				}
			});
			stream.on("data", (chunk: unknown) => {
				stream.pause();
				chunks.push(chunk as Uint8Array);
				notify?.();
			});
			stream.once("trailers", (headers: unknown) => {
				let headers_ = new Headers(headers as tg.Host.Http2.Headers);
				if (headers_.get("x-tg-event") === "error") {
					let data = headers_.get("x-tg-data");
					if (data === undefined) {
						fail(new Error("missing data"));
					} else {
						try {
							fail(tg.Error.fromData(JSON.parse(data) as tg.Error.Data));
						} catch (error) {
							fail(error);
						}
					}
				}
			});
			stream.once("close", () => {
				if (!done)
					fail(
						Object.assign(
							new Error("the HTTP/2 stream closed before the response ended"),
							{ code: "ERR_HTTP2_STREAM_CLOSED" },
						),
					);
			});
			stream.once("end", () => {
				if (!settled) {
					fail(
						Object.assign(
							new Error("the response ended before its headers arrived"),
							{ code: "ERR_HTTP2_STREAM_CLOSED" },
						),
					);
					return;
				}
				done = true;
				notify?.();
			});
		});
	}

	onClose(callback: () => void): this {
		let close = this.#close;
		let called = false;
		this.#close = () => {
			close?.();
			if (!called) {
				called = true;
				callback();
			}
		};
		let body = this.body;
		let response = this;
		this.body = new Body({
			async *[Symbol.asyncIterator]() {
				try {
					yield* body;
				} finally {
					response.close();
				}
			},
		});
		return this;
	}

	close(): void {
		let close = this.#close;
		this.#close = undefined;
		close?.();
	}

	async collect() {
		try {
			return await this.body.collect();
		} finally {
			this.close();
		}
	}

	async bodyHeader<T = unknown>(): Promise<T> {
		let source = this.body[Symbol.asyncIterator]();
		let buffer: Uint8Array = new Uint8Array();
		let offset = 0;
		let read = async (length: number) => {
			let bytes = new Uint8Array(length);
			let position = 0;
			while (position < length) {
				if (offset === buffer.length) {
					let next = await source.next();
					if (next.done)
						throw new Error("the response ended inside the header");
					buffer = next.value;
					offset = 0;
					continue;
				}
				let count = Math.min(length - position, buffer.length - offset);
				bytes.set(buffer.subarray(offset, offset + count), position);
				offset += count;
				position += count;
			}
			return bytes;
		};
		try {
			requireJson(this.headers.get("content-type"));
			let length = 0;
			for (let index = 0; ; index++) {
				if (index === 10) throw new Error("invalid header length");
				let byte = (await read(1))[0]!;
				length += (byte & 127) * 2 ** (7 * index);
				if (length > 1_048_576) throw new Error("header too large");
				if (byte < 128) break;
			}
			let header = JSON.parse(tg.encoding.utf8.decode(await read(length))) as T;
			this.body = new Body({
				async *[Symbol.asyncIterator]() {
					try {
						if (offset < buffer.length) yield buffer.subarray(offset);
						while (true) {
							let next = await source.next();
							if (next.done) break;
							yield next.value;
						}
					} finally {
						await source.return?.();
					}
				},
			});
			return header;
		} catch (error) {
			await source.return?.();
			this.close();
			throw error;
		}
	}

	async json<T = unknown>() {
		try {
			return await this.body.json<T>();
		} finally {
			this.close();
		}
	}

	async *sse() {
		try {
			yield* this.body.sse();
		} finally {
			this.close();
		}
	}
}
