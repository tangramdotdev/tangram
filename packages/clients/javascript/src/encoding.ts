export let encoding: Encoding = {} as any;

export let setEncoding = (newEncoding: Encoding) => {
	Object.defineProperties(
		encoding,
		Object.getOwnPropertyDescriptors(newEncoding),
	);
};

export type Encoding = {
	base64: {
		decode(value: string): Uint8Array;
		encode(value: Uint8Array): string;
	};
	hex: {
		decode(value: string): Uint8Array;
		encode(value: Uint8Array): string;
	};
	json: {
		decode(value: string): unknown;
		encode(value: unknown): string;
	};
	toml: {
		decode(value: string): unknown;
		encode(value: unknown): string;
	};
	utf8: {
		decode(value: Uint8Array): string;
		encode(value: string): Uint8Array;
	};
	yaml: {
		decode(value: string): unknown;
		encode(value: unknown): string;
	};
};
