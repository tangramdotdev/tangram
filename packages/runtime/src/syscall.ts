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
	return await syscall("archive", artifact, format);
};

export let build = async (target: Target): Promise<Value> => {
	return await syscall("build", target);
};

export let bundle = async (artifact: Artifact): Promise<Directory> => {
	return await syscall("bundle", artifact);
};

export let checksum = (
	algorithm: Checksum.Algorithm,
	bytes: string | Uint8Array,
): Checksum => {
	return syscall("checksum", algorithm, bytes);
};

export let compress = async (
	blob: Blob,
	format: Blob.CompressionFormat,
): Promise<Blob> => {
	return await syscall("compress", blob, format);
};

export let decompress = async (
	blob: Blob,
	format: Blob.CompressionFormat,
): Promise<Blob> => {
	return await syscall("decompress", blob, format);
};

export let download = async (
	url: string,
	checksum: Checksum,
): Promise<Blob> => {
	return await syscall("download", url, checksum);
};

export let encoding = {
	base64: {
		decode: (value: string): Uint8Array => {
			return syscall("encoding_base64_decode", value);
		},

		encode: (value: Uint8Array): string => {
			return syscall("encoding_base64_encode", value);
		},
	},

	hex: {
		decode: (value: string): Uint8Array => {
			return syscall("encoding_hex_decode", value);
		},

		encode: (value: Uint8Array): string => {
			return syscall("encoding_hex_encode", value);
		},
	},

	json: {
		decode: (value: string): unknown => {
			return syscall("encoding_json_decode", value);
		},

		encode: (value: any): string => {
			return syscall("encoding_json_encode", value);
		},
	},

	toml: {
		decode: (value: string): unknown => {
			return syscall("encoding_toml_decode", value);
		},

		encode: (value: any): string => {
			return syscall("encoding_toml_encode", value);
		},
	},

	utf8: {
		decode: (value: Uint8Array): string => {
			return syscall("encoding_utf8_decode", value);
		},

		encode: (value: string): Uint8Array => {
			return syscall("encoding_utf8_encode", value);
		},
	},

	yaml: {
		decode: (value: string): unknown => {
			return syscall("encoding_yaml_decode", value);
		},

		encode: (value: any): string => {
			return syscall("encoding_yaml_encode", value);
		},
	},
};

export let extract = async (
	blob: Blob,
	format: Blob.ArchiveFormat,
): Promise<Artifact> => {
	return await syscall("extract", blob, format);
};

export let log = (value: string) => {
	return syscall("log", value);
};

export let load = async (id: Object_.Id): Promise<Object_.Object_> => {
	return await syscall("load", id);
};

export let read = async (blob: Blob): Promise<Uint8Array> => {
	return await syscall("read", blob);
};

export let store = async (object: Object_.Object_): Promise<Object_.Id> => {
	return await syscall("store", object);
};

export let sleep = async (duration: number): Promise<void> => {
	return await syscall("sleep", duration);
};
