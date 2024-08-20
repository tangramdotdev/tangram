import type * as tg from "./index.ts";

declare global {
	function syscall(
		syscall: "checksum",
		input: string | Uint8Array,
		algorithm: tg.Checksum.Algorithm,
	): tg.Checksum;

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
		syscall: "load",
		id: tg.Object.Id,
	): Promise<tg.Object.Object>;

	function syscall(syscall: "log", value: string): void;

	function syscall(syscall: "output", target: tg.Target): Promise<tg.Value>;

	function syscall(syscall: "read", blob: tg.Blob): Promise<Uint8Array>;

	function syscall(syscall: "sleep", duration: number): Promise<void>;

	function syscall(
		syscall: "store",
		object: tg.Object.Object,
	): Promise<tg.Object.Id>;
}
