import * as tg from "../index.ts";
import { Body } from "./body.ts";
import { Headers } from "./headers.ts";
import { Uri } from "./uri.ts";

export class Request {
	body?: Body;
	headers: Headers;
	method: string;
	uri: Uri;

	constructor(arg: Request.Arg) {
		if (arg.body !== undefined) {
			this.body = arg.body instanceof Body ? arg.body : new Body(arg.body);
		}
		this.headers =
			arg.headers instanceof Headers ? arg.headers : new Headers(arg.headers);
		this.method = arg.method;
		this.uri = arg.uri instanceof Uri ? arg.uri : new Uri(arg.uri);
	}

	arg(arg: Record<string, Uri.QueryValue>, body = this.body ?? Body.empty()) {
		let uri = new Uri({ path: this.uri.path, query: arg });
		let headers = this.headers.toData();
		if ((uri.query?.length ?? 0) > 4096) {
			let bytes = tg.encoding.utf8.encode(JSON.stringify(arg));
			let length = bytes.length;
			let prefix: Array<number> = [];
			while (length >= 128) {
				prefix.push((length % 128) | 128);
				length = Math.floor(length / 128);
			}
			prefix.push(length);
			let frame = new Uint8Array(prefix.length + bytes.length);
			frame.set(prefix);
			frame.set(bytes, prefix.length);
			body = body.prepend(frame);
			delete uri.query;
			headers["x-tg-arg-in-body"] = "true";
			headers["cache-control"] = "no-store";
			delete headers["content-length"];
		} else {
			delete headers["x-tg-arg-in-body"];
		}
		this.body = body;
		this.headers = new Headers(headers);
		this.uri = uri;
		return this;
	}
}

export namespace Request {
	export type Arg = {
		body?: Body | AsyncIterable<string | Uint8Array>;
		headers?: Headers | tg.Host.Http2.Headers;
		method: string;
		uri: Uri | Uri.Arg;
	};
}
