import * as syscall from "./syscall.ts";

export namespace base64 {
	export let decode = (value: string): Uint8Array => {
		return syscall.encoding.base64.decode(value);
	};

	export let encode = (value: Uint8Array): string => {
		return syscall.encoding.base64.encode(value);
	};
}

export namespace hex {
	export let decode = (value: string): Uint8Array => {
		return syscall.encoding.hex.decode(value);
	};

	export let encode = (value: Uint8Array): string => {
		return syscall.encoding.hex.encode(value);
	};
}

export namespace json {
	export let decode = (value: string): unknown => {
		return syscall.encoding.json.decode(value);
	};

	export let encode = (value: any): string => {
		return syscall.encoding.json.encode(value);
	};
}

export namespace toml {
	export let decode = (value: string): unknown => {
		return syscall.encoding.toml.decode(value);
	};

	export let encode = (value: any): string => {
		return syscall.encoding.toml.encode(value);
	};
}

export namespace utf8 {
	export let decode = (value: Uint8Array): string => {
		return syscall.encoding.utf8.decode(value);
	};

	export let encode = (value: string): Uint8Array => {
		return syscall.encoding.utf8.encode(value);
	};
}

export namespace yaml {
	export let decode = (value: string): unknown => {
		return syscall.encoding.yaml.decode(value);
	};

	export let encode = (value: any): string => {
		return syscall.encoding.yaml.encode(value);
	};
}
