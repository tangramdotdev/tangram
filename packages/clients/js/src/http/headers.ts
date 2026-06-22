import * as tg from "../index.ts";

export class Headers {
	#headers: tg.Host.Http2.Headers;

	constructor(headers?: Headers | tg.Host.Http2.Headers | undefined) {
		this.#headers =
			headers instanceof Headers ? headers.toData() : (headers ?? {});
	}

	get(name: string): string | undefined {
		let value = this.#headers[name];
		if (Array.isArray(value)) {
			return value[0];
		}
		if (typeof value === "number") {
			return value.toString();
		}
		return value;
	}

	toData(): tg.Host.Http2.Headers {
		return { ...this.#headers };
	}
}
