import * as tg from "./index.ts";

export namespace base64 {
	export let decode = (value: string): Uint8Array => {
		return tg.handle.encoding.base64.decode(value);
	};

	export let encode = (value: Uint8Array): string => {
		return tg.handle.encoding.base64.encode(value);
	};
}

export namespace hex {
	export let decode = (value: string): Uint8Array => {
		return tg.handle.encoding.hex.decode(value);
	};

	export let encode = (value: Uint8Array): string => {
		return tg.handle.encoding.hex.encode(value);
	};
}

export namespace json {
	export let decode = (value: string): unknown => {
		return tg.handle.encoding.json.decode(value);
	};

	export let encode = (value: unknown): string => {
		return tg.handle.encoding.json.encode(value);
	};
}

export namespace toml {
	export let decode = (value: string): unknown => {
		return tg.handle.encoding.toml.decode(value);
	};

	export let encode = (value: unknown): string => {
		return tg.handle.encoding.toml.encode(value);
	};
}

export namespace utf8 {
	export let decode = (value: Uint8Array): string => {
		return tg.handle.encoding.utf8.decode(value);
	};

	export let encode = (value: string): Uint8Array => {
		return tg.handle.encoding.utf8.encode(value);
	};
}

export namespace yaml {
	export let decode = (value: string): unknown => {
		return tg.handle.encoding.yaml.decode(value);
	};

	export let encode = (value: unknown): string => {
		return tg.handle.encoding.yaml.encode(value);
	};
}
