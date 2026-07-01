import * as tg from "../index.ts";
import { Body, parseJson } from "./body.ts";
import { Headers } from "./headers.ts";

export class Response {
	body: Body;
	headers: Headers;
	status: number;

	constructor(
		status: number,
		headers: Headers | tg.Host.Http2.Headers,
		body: Body,
	) {
		this.body = body;
		this.headers = headers instanceof Headers ? headers : new Headers(headers);
		this.status = status;
	}

	static fromStream(stream: tg.Host.Http2.ClientHttp2Stream) {
		let chunks: Array<Uint8Array> = [];
		let error: unknown;
		let notify: (() => void) | undefined;
		let done = false;
		let settled = false;

		let body = new Body({
			async *[Symbol.asyncIterator]() {
				try {
					while (true) {
						if (chunks.length > 0) {
							yield chunks.shift()!;
						} else if (error !== undefined) {
							throw error;
						} else if (done) {
							break;
						} else {
							await new Promise<void>((resolve) => {
								notify = resolve;
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
				done = true;
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
					resolve(new Response(status, headers_, body));
				} catch (error) {
					fail(error);
				}
			});
			stream.on("data", (chunk: unknown) => {
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
						fail(tg.Error.fromData(parseJson(data) as tg.Error.Data));
					}
				}
			});
			stream.once("end", () => {
				done = true;
				notify?.();
			});
		});
	}

	async collect() {
		return await this.body.collect();
	}

	async json<T = unknown>() {
		return await this.body.json<T>();
	}

	sse() {
		return this.body.sse();
	}
}
