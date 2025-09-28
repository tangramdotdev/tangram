import type * as tg from "./index.ts";

declare global {
	function syscall(
		syscall: "blob_create",
		bytes: string | Uint8Array,
	): Promise<tg.Blob.Id>;

	function syscall(
		syscall: "blob_read",
		blob: tg.Blob.Id,
		arg: BlobReadArg,
	): Promise<Uint8Array>;

	type BlobReadArg = {
		position?: number | string | undefined;
		length?: number | undefined;
	};

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
		arg: SpawnArg,
	): Promise<SpawnOutput>;

	type SpawnArg = {
		checksum: tg.Checksum | undefined;
		command: tg.Referent<tg.Command.Id>;
		create: boolean;
		mounts: Array<tg.Process.Mount>;
		network: boolean | undefined;
		parent: tg.Process.Id | undefined;
		remote: string | undefined;
		retry: boolean;
		stderr: string | undefined;
		stdin: string | undefined;
		stdout: string | undefined;
	};

	type SpawnOutput = {
		process: tg.Process.Id;
		remote: string | undefined;
	};

	function syscall(
		syscall: "process_wait",
		id: tg.Process.Id,
		remote: string | undefined,
	): Promise<tg.Process.Wait.Data>;

	function syscall(syscall: "sleep", duration: number): Promise<void>;

	function syscall(syscall: "sync", items: Array<SyncItem>): Promise<void>;

	type SyncItem = {
		id: tg.Object.Id;
		data: tg.Object.Data;
	};
}
