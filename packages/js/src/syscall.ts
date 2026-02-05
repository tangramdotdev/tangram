import type * as tg from "@tangramdotdev/client";

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
		syscall: "log",
		stream: "stdout" | "stderr",
		string: string,
	): void;

	function syscall(
		syscall: "magic",
		value: Function,
	): tg.Command.Data.Executable;

	function syscall(
		syscall: "object_batch",
		arg: tg.Handle.PostObjectBatchArg,
	): Promise<void>;

	function syscall(
		syscall: "object_get",
		id: tg.Object.Id,
	): Promise<tg.Object.Data>;

	function syscall(syscall: "object_id", object: tg.Object.Data): tg.Object.Id;

	function syscall(
		syscall: "process_get",
		id: tg.Process.Id,
		remote: string | undefined,
	): Promise<tg.Process.Data>;

	function syscall(
		syscall: "process_spawn",
		arg: tg.Handle.SpawnArg,
	): Promise<tg.Handle.SpawnOutput>;

	function syscall(
		syscall: "process_wait",
		id: tg.Process.Id,
		arg: tg.Handle.WaitArg,
	): Promise<tg.Process.Wait.Data>;

	function syscall(
		syscall: "read",
		arg: tg.Handle.ReadArg,
	): Promise<Uint8Array>;

	function syscall(syscall: "sleep", duration: number): Promise<void>;

	function syscall(
		syscall: "write",
		bytes: string | Uint8Array,
	): Promise<tg.Blob.Id>;
}
