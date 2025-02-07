import type * as tg from "./index.ts";

declare global {
	function syscall(
		syscall: "blob_create",
		bytes: string | Uint8Array,
	): Promise<tg.Blob>;

	function syscall(syscall: "blob_read", blob: tg.Blob): Promise<Uint8Array>;

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

	function syscall(syscall: "log", contents: string, level: string): void;

	function syscall(syscall: "module_load", module: tg.Module): Promise<any>;

	function syscall(
		syscall: "object_load",
		id: tg.Object.Id,
	): Promise<tg.Object.Object>;

	function syscall(
		syscall: "object_store",
		object: tg.Object.Object,
	): Promise<tg.Object.Id>;

	function syscall(
		syscall: "process_load",
		id: tg.Process.Id,
		remote: string | undefined,
	): Promise<tg.Process.State>;

	function syscall(
		syscall: "process_spawn",
		arg: {
			checksum: tg.Checksum | undefined;
			command: tg.Command.Id | undefined;
			create: boolean;
			cwd: string | undefined;
			env: { [key: string]: string } | undefined;
			network: boolean | undefined;
			parent: tg.Process.Id | undefined;
			remote: string | undefined;
			retry: boolean;
		},
	): Promise<{ process: tg.Process.Id; remote: string | undefined }>;

	function syscall(
		syscall: "process_wait",
		id: tg.Process.Id,
		remote: string | undefined,
	): Promise<tg.Process.WaitOutput>;

	function syscall(syscall: "sleep", duration: number): Promise<void>;
}
