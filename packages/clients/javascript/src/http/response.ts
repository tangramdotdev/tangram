import * as tg from "../index.ts";
import { Body } from "./body.ts";
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
