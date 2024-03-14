import { Artifact } from "./artifact.ts";
import { Blob } from "./blob.ts";
import { Checksum } from "./checksum.ts";
import { Directory } from "./directory.ts";
import { Object_ } from "./object.ts";
import { Target } from "./target.ts";
import { Value } from "./value.ts";

declare global {
	function syscall(
		syscall: "archive",
		artifact: Artifact,
		format: Blob.ArchiveFormat,
	): Promise<Blob>;

	function syscall(syscall: "build", target: Target): Promise<Value>;

	function syscall(syscall: "bundle", artifact: Artifact): Promise<Directory>;

	function syscall(
		syscall: "checksum",
		algorithm: Checksum.Algorithm,
		bytes: string | Uint8Array,
	): Checksum;

	function syscall(
		syscall: "compress",
		blob: Blob,
		format: Blob.CompressionFormat,
	): Promise<Blob>;

	function syscall(
		syscall: "decompress",
		blob: Blob,
		format: Blob.CompressionFormat,
	): Promise<Blob>;

	function syscall(
		syscall: "download",
		url: string,
		checksum: Checksum,
	): Promise<Blob>;

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

	function syscall(
		syscall: "extract",
		blob: Blob,
		format: Blob.ArchiveFormat,
	): Promise<Artifact>;

	function syscall(syscall: "load", id: Object_.Id): Promise<Object_.Object_>;

	function syscall(syscall: "log", value: string): void;

	function syscall(syscall: "read", blob: Blob): Promise<Uint8Array>;

	function syscall(
		syscall: "store",
		object: Object_.Object_,
	): Promise<Object_.Id>;

	function syscall(syscall: "sleep", duration: number): Promise<void>;
}

export let archive = async (
	artifact: Artifact,
	format: Blob.ArchiveFormat,
): Promise<Blob> => {
	try {
		return await syscall("archive", artifact, format);
	} catch (cause) {
		throw new Error("the syscall failed", { cause });
	}
};

export let build = async (target: Target): Promise<Value> => {
	try {
		return await syscall("build", target);
	} catch (cause) {
		throw new Error("the syscall failed", { cause });
	}
};

export let bundle = async (artifact: Artifact): Promise<Directory> => {
	try {
		return await syscall("bundle", artifact);
	} catch (cause) {
		throw new Error("the syscall failed", { cause });
	}
};

export let checksum = (
	algorithm: Checksum.Algorithm,
	bytes: string | Uint8Array,
): Checksum => {
	try {
		return syscall("checksum", algorithm, bytes);
	} catch (cause) {
		throw new Error("the syscall failed", { cause });
	}
};

export let compress = async (
	blob: Blob,
	format: Blob.CompressionFormat,
): Promise<Blob> => {
	try {
		return await syscall("compress", blob, format);
	} catch (cause) {
		throw new Error("the syscall failed", { cause });
	}
};

export let decompress = async (
	blob: Blob,
	format: Blob.CompressionFormat,
): Promise<Blob> => {
	try {
		return await syscall("decompress", blob, format);
	} catch (cause) {
		throw new Error("the syscall failed", { cause });
	}
};

export let download = async (
	url: string,
	checksum: Checksum,
): Promise<Blob> => {
	try {
		return await syscall("download", url, checksum);
	} catch (cause) {
		throw new Error("the syscall failed", { cause });
	}
};

export let encoding = {
	base64: {
		decode: (value: string): Uint8Array => {
			try {
				return syscall("encoding_base64_decode", value);
			} catch (cause) {
				throw new Error("the syscall failed", { cause });
			}
		},

		encode: (value: Uint8Array): string => {
			try {
				return syscall("encoding_base64_encode", value);
			} catch (cause) {
				throw new Error("the syscall failed", { cause });
			}
		},
	},

	hex: {
		decode: (value: string): Uint8Array => {
			try {
				return syscall("encoding_hex_decode", value);
			} catch (cause) {
				throw new Error("the syscall failed", { cause });
			}
		},

		encode: (value: Uint8Array): string => {
			try {
				return syscall("encoding_hex_encode", value);
			} catch (cause) {
				throw new Error("the syscall failed", { cause });
			}
		},
	},

	json: {
		decode: (value: string): unknown => {
			try {
				return syscall("encoding_json_decode", value);
			} catch (cause) {
				throw new Error("the syscall failed", { cause });
			}
		},

		encode: (value: any): string => {
			try {
				return syscall("encoding_json_encode", value);
			} catch (cause) {
				throw new Error("the syscall failed", { cause });
			}
		},
	},

	toml: {
		decode: (value: string): unknown => {
			try {
				return syscall("encoding_toml_decode", value);
			} catch (cause) {
				throw new Error("the syscall failed", { cause });
			}
		},

		encode: (value: any): string => {
			try {
				return syscall("encoding_toml_encode", value);
			} catch (cause) {
				throw new Error("the syscall failed", { cause });
			}
		},
	},

	utf8: {
		decode: (value: Uint8Array): string => {
			try {
				return syscall("encoding_utf8_decode", value);
			} catch (cause) {
				throw new Error("the syscall failed", { cause });
			}
		},

		encode: (value: string): Uint8Array => {
			try {
				return syscall("encoding_utf8_encode", value);
			} catch (cause) {
				throw new Error("the syscall failed", { cause });
			}
		},
	},

	yaml: {
		decode: (value: string): unknown => {
			try {
				return syscall("encoding_yaml_decode", value);
			} catch (cause) {
				throw new Error("the syscall failed", { cause });
			}
		},

		encode: (value: any): string => {
			try {
				return syscall("encoding_yaml_encode", value);
			} catch (cause) {
				throw new Error("the syscall failed", { cause });
			}
		},
	},
};

export let extract = async (
	blob: Blob,
	format: Blob.ArchiveFormat,
): Promise<Artifact> => {
	try {
		return await syscall("extract", blob, format);
	} catch (cause) {
		throw new Error("the syscall failed", { cause });
	}
};

export let log = (value: string) => {
	try {
		return syscall("log", value);
	} catch (cause) {
		throw new Error("the syscall failed", { cause });
	}
};

export let load = async (id: Object_.Id): Promise<Object_.Object_> => {
	try {
		return await syscall("load", id);
	} catch (cause) {
		throw new Error("the syscall failed", { cause });
	}
};

export let read = async (blob: Blob): Promise<Uint8Array> => {
	try {
		return await syscall("read", blob);
	} catch (cause) {
		throw new Error("the syscall failed", { cause });
	}
};

export let store = async (object: Object_.Object_): Promise<Object_.Id> => {
	try {
		return await syscall("store", object);
	} catch (cause) {
		throw new Error("the syscall failed", { cause });
	}
};

export let sleep = async (duration: number): Promise<void> => {
	try {
		return await syscall("sleep", duration);
	} catch (cause) {
		throw new Error("the syscall failed", { cause });
	}
};
