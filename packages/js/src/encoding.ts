import type * as tg from "@tangramdotdev/client";

export let encoding: tg.Encoding = {
	base64: {
		decode(value: string): Uint8Array {
			return syscall("encoding_base64_decode", value);
		},
		encode(value: Uint8Array): string {
			return syscall("encoding_base64_encode", value);
		},
	},

	hex: {
		decode(value: string): Uint8Array {
			return syscall("encoding_hex_decode", value);
		},
		encode(value: Uint8Array): string {
			return syscall("encoding_hex_encode", value);
		},
	},

	json: {
		decode(value: string): unknown {
			return syscall("encoding_json_decode", value);
		},
		encode(value: unknown): string {
			return syscall("encoding_json_encode", value);
		},
	},

	toml: {
		decode(value: string): unknown {
			return syscall("encoding_toml_decode", value);
		},
		encode(value: unknown): string {
			return syscall("encoding_toml_encode", value);
		},
	},

	utf8: {
		decode(value: Uint8Array): string {
			return syscall("encoding_utf8_decode", value);
		},
		encode(value: string): Uint8Array {
			return syscall("encoding_utf8_encode", value);
		},
	},

	yaml: {
		decode(value: string): unknown {
			return syscall("encoding_yaml_decode", value);
		},
		encode(value: unknown): string {
			return syscall("encoding_yaml_encode", value);
		},
	},
};
