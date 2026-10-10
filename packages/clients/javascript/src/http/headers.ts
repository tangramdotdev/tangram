import * as tg from "../index.ts";

export class Headers {
	#headers: tg.Host.Http2.Headers;

	constructor(headers?: Headers | tg.Host.Http2.Headers) {
		this.#headers = Object.fromEntries(
			Object.entries(
				headers instanceof Headers ? headers.toData() : (headers ?? {}),
			).map(([name, value]) => [name.toLowerCase(), value]),
		);
	}

	get(name: string): string | undefined {
		let value = this.#headers[name.toLowerCase()];
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
