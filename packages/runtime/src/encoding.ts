export namespace base64 {
	export let decode = (value: string): Uint8Array => {
		return syscall("encoding_base64_decode", value);
	};

	export let encode = (value: Uint8Array): string => {
		return syscall("encoding_base64_encode", value);
	};
}

export namespace hex {
	export let decode = (value: string): Uint8Array => {
		return syscall("encoding_hex_decode", value);
	};

	export let encode = (value: Uint8Array): string => {
		return syscall("encoding_hex_encode", value);
	};
}

export namespace json {
	export let decode = (value: string): unknown => {
		return syscall("encoding_json_decode", value);
	};

	export let encode = (value: unknown): string => {
		return syscall("encoding_json_encode", value);
	};
}

export namespace toml {
	export let decode = (value: string): unknown => {
		return syscall("encoding_toml_decode", value);
	};

	export let encode = (value: unknown): string => {
		return syscall("encoding_toml_encode", value);
	};
}

export namespace utf8 {
	export let decode = (value: Uint8Array): string => {
		return syscall("encoding_utf8_decode", value);
	};

	export let encode = (value: string): Uint8Array => {
		return syscall("encoding_utf8_encode", value);
	};
}

export namespace yaml {
	export let decode = (value: string): unknown => {
		return syscall("encoding_yaml_decode", value);
	};

	export let encode = (value: unknown): string => {
		return syscall("encoding_yaml_encode", value);
	};
}
