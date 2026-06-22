import * as tg from "../index.ts";
import { Body } from "./body.ts";
import { Headers } from "./headers.ts";
import { Uri } from "./uri.ts";

export class Request {
	body?: Body | undefined;
	headers: Headers;
	method: string;
	uri: Uri;

	constructor(arg: Request.Arg) {
		this.body =
			arg.body === undefined
				? undefined
				: arg.body instanceof Body
					? arg.body
					: new Body(arg.body);
		this.headers =
			arg.headers instanceof Headers ? arg.headers : new Headers(arg.headers);
		this.method = arg.method;
		this.uri = arg.uri instanceof Uri ? arg.uri : new Uri(arg.uri);
	}
}

export namespace Request {
	export type Arg = {
		body?: Body | AsyncIterable<string | Uint8Array> | undefined;
		headers?: Headers | tg.Host.Http2.Headers | undefined;
		method: string;
		uri: Uri | Uri.Arg;
	};
}
