import { Module } from "./module.ts";

declare global {
	/** Get the modules for all documents. */
	function syscall(name: "documents"): Array<Module>;

	function syscall(
		syscall: "encoding_base64_decode",
		value: string,
	): Uint8Array;

	function syscall(
		syscall: "encoding_base64_encode",
		value: Uint8Array,
	): string;

	function syscall(syscall: "encoding_hex_decode", value: string): Uint8Array;

	function syscall(syscall: "encoding_hex_encode", value: Uint8Array): string;

	function syscall(syscall: "encoding_json_decode", value: string): unknown;

	function syscall(syscall: "encoding_json_encode", value: any): string;

	function syscall(syscall: "encoding_toml_decode", value: string): unknown;

	function syscall(syscall: "encoding_toml_encode", value: any): string;

	function syscall(syscall: "encoding_utf8_decode", value: Uint8Array): string;

	function syscall(syscall: "encoding_utf8_encode", value: string): Uint8Array;

	function syscall(syscall: "encoding_yaml_decode", value: string): unknown;

	function syscall(syscall: "encoding_yaml_encode", value: any): string;

	/** Write to the log. */
	function syscall(syscall: "log", value: string): void;

	/** Load a module. */
	function syscall(name: "module_load", module: Module): string;

	/** Resolve a module. */
	function syscall(
		name: "module_resolve",
		module: Module,
		specifier: string,
		attributes: { [key: string]: string },
	): Module;

	/** Get the version of a module. */
	function syscall(name: "module_version", module: Module): string;
}

export let documents = (): Array<Module> => {
	try {
		return syscall("documents");
	} catch (cause) {
		throw new Error("The syscall failed.", { cause });
	}
};

export let encoding = {
	base64: {
		decode: (value: string): Uint8Array => {
			try {
				return syscall("encoding_base64_decode", value);
			} catch (cause) {
				throw new Error("The syscall failed.", { cause });
			}
		},

		encode: (value: Uint8Array): string => {
			try {
				return syscall("encoding_base64_encode", value);
			} catch (cause) {
				throw new Error("The syscall failed.", { cause });
			}
		},
	},

	hex: {
		decode: (value: string): Uint8Array => {
			try {
				return syscall("encoding_hex_decode", value);
			} catch (cause) {
				throw new Error("The syscall failed.", { cause });
			}
		},

		encode: (value: Uint8Array): string => {
			try {
				return syscall("encoding_hex_encode", value);
			} catch (cause) {
				throw new Error("The syscall failed.", { cause });
			}
		},
	},

	json: {
		decode: (value: string): unknown => {
			try {
				return syscall("encoding_json_decode", value);
			} catch (cause) {
				throw new Error("The syscall failed.", { cause });
			}
		},

		encode: (value: any): string => {
			try {
				return syscall("encoding_json_encode", value);
			} catch (cause) {
				throw new Error("The syscall failed.", { cause });
			}
		},
	},

	toml: {
		decode: (value: string): unknown => {
			try {
				return syscall("encoding_toml_decode", value);
			} catch (cause) {
				throw new Error("The syscall failed.", { cause });
			}
		},

		encode: (value: any): string => {
			try {
				return syscall("encoding_toml_encode", value);
			} catch (cause) {
				throw new Error("The syscall failed.", { cause });
			}
		},
	},

	utf8: {
		decode: (value: Uint8Array): string => {
			try {
				return syscall("encoding_utf8_decode", value);
			} catch (cause) {
				throw new Error("The syscall failed.", { cause });
			}
		},

		encode: (value: string): Uint8Array => {
			try {
				return syscall("encoding_utf8_encode", value);
			} catch (cause) {
				throw new Error("The syscall failed.", { cause });
			}
		},
	},

	yaml: {
		decode: (value: string): unknown => {
			try {
				return syscall("encoding_yaml_decode", value);
			} catch (cause) {
				throw new Error("The syscall failed.", { cause });
			}
		},

		encode: (value: any): string => {
			try {
				return syscall("encoding_yaml_encode", value);
			} catch (cause) {
				throw new Error("The syscall failed.", { cause });
			}
		},
	},
};

export let log = (value: string) => {
	try {
		return syscall("log", value);
	} catch (cause) {
		throw new Error("The syscall failed.", { cause });
	}
};

export let module_ = {
	load: (module: Module): string => {
		try {
			return syscall("module_load", module);
		} catch (cause) {
			throw new Error("The syscall failed.", { cause });
		}
	},

	resolve: (
		module: Module,
		specifier: string,
		attributes: { [key: string]: string },
	): Module => {
		try {
			return syscall("module_resolve", module, specifier, attributes);
		} catch (cause) {
			throw new Error("The syscall failed.", { cause });
		}
	},

	version: (module: Module) => {
		try {
			return syscall("module_version", module);
		} catch (cause) {
			throw new Error("The syscall failed.", { cause });
		}
	},
};
