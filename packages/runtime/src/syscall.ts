import type { Artifact } from "./artifact.ts";
import type { Blob } from "./blob.ts";
import type { Checksum } from "./checksum.ts";
import type { Directory } from "./directory.ts";
import type { Object_ } from "./object.ts";
import type { Target } from "./target.ts";
import type { Value } from "./value.ts";

declare global {
	function syscall(
		syscall: "archive",
		artifact: Artifact,
		format: Artifact.ArchiveFormat,
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

	function syscall(syscall: "encoding_json_encode", value: unknown): string;

	function syscall(syscall: "encoding_toml_decode", value: string): unknown;

	function syscall(syscall: "encoding_toml_encode", value: unknown): string;

	function syscall(syscall: "encoding_utf8_decode", value: Uint8Array): string;

	function syscall(syscall: "encoding_utf8_encode", value: string): Uint8Array;

	function syscall(syscall: "encoding_yaml_decode", value: string): unknown;

	function syscall(syscall: "encoding_yaml_encode", value: unknown): string;

	function syscall(
		syscall: "extract",
		blob: Blob,
		format: Artifact.ArchiveFormat,
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
