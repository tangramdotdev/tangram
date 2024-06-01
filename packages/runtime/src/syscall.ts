import type { Blob } from "./blob.ts";
import type { Checksum } from "./checksum.ts";
import type { Object_ } from "./object.ts";
import type { Target } from "./target.ts";
import type { Value } from "./value.ts";

declare global {
	function syscall(
		syscall: "checksum",
		input: string | Uint8Array,
		algorithm: Checksum.Algorithm,
	): Checksum;

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

	function syscall(syscall: "load", id: Object_.Id): Promise<Object_.Object_>;

	function syscall(syscall: "log", value: string): void;

	function syscall(syscall: "output", target: Target): Promise<Value>;

	function syscall(syscall: "read", blob: Blob): Promise<Uint8Array>;

	function syscall(syscall: "sleep", duration: number): Promise<void>;

	function syscall(
		syscall: "store",
		object: Object_.Object_,
	): Promise<Object_.Id>;
}
