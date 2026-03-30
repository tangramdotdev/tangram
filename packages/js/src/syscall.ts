import type * as tg from "@tangramdotdev/client";

declare global {
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
		syscall: "handle_checkin",
		arg: tg.Handle.CheckinArg,
	): Promise<tg.Artifact.Id>;

	function syscall(
		syscall: "handle_checksum",
		input: string | Uint8Array,
		algorithm: tg.Checksum.Algorithm,
	): tg.Checksum;

	function syscall(
		syscall: "handle_checkout",
		arg: tg.Handle.CheckoutArg,
	): Promise<string>;

	function syscall(
		syscall: "handle_object_batch",
		arg: tg.Handle.PostObjectBatchArg,
	): Promise<void>;

	function syscall(
		syscall: "handle_object_get",
		id: tg.Object.Id,
	): Promise<tg.Object.Data>;

	function syscall(syscall: "handle_object_id", object: tg.Object.Data): tg.Object.Id;

	function syscall(
		syscall: "handle_process_get",
		id: tg.Process.Id,
		remote: string | undefined,
	): Promise<tg.Process.Data>;

	function syscall(syscall: "handle_process_id", value: undefined): tg.Process.Id;

	function syscall(
		syscall: "handle_process_signal",
		id: tg.Process.Id,
		arg: tg.Handle.SignalArg,
	): Promise<void>;

	function syscall(
		syscall: "handle_process_spawn",
		arg: tg.Handle.SpawnArg,
	): Promise<tg.Handle.SpawnOutput>;

	function syscall(
		syscall: "handle_process_stdio_read_close",
		token: number,
	): Promise<void>;

	function syscall(
		syscall: "handle_process_stdio_read_open",
		id: tg.Process.Id,
		arg: tg.Handle.ProcessStdioReadArg,
	): Promise<number | undefined>;

	function syscall(
		syscall: "handle_process_stdio_read_read",
		token: number,
	): Promise<tg.Process.Stdio.Read.Event | undefined>;

	function syscall(
		syscall: "handle_process_stdio_write_close",
		token: number,
	): Promise<void>;

	function syscall(
		syscall: "handle_process_stdio_write_open",
		id: tg.Process.Id,
		arg: tg.Handle.ProcessStdioWriteArg,
	): Promise<number>;

	function syscall(
		syscall: "handle_process_stdio_write_write",
		token: number,
		chunk: {
			bytes: string | Uint8Array;
			position?: number | undefined;
			stream: tg.Process.Stdio.Stream;
		},
	): Promise<void>;

	function syscall(
		syscall: "handle_process_tty_size_put",
		id: tg.Process.Id,
		arg: tg.Handle.ProcessTtySizePutArg,
	): Promise<void>;

	function syscall(
		syscall: "handle_process_wait",
		id: tg.Process.Id,
		arg: tg.Handle.WaitArg,
	): Promise<tg.Process.Wait.Data>;

	function syscall(
		syscall: "handle_read",
		arg: tg.Handle.ReadArg,
	): Promise<Uint8Array>;

	function syscall(syscall: "handle_value_parse", value: string): tg.Value.Data;

	function syscall(
		syscall: "handle_value_stringify",
		value: tg.Value.Data,
	): string;

	function syscall(
		syscall: "handle_write",
		bytes: string | Uint8Array,
	): Promise<tg.Blob.Id>;

	function syscall(syscall: "host_close", fd: number): Promise<void>;

	function syscall(syscall: "host_exists", path: string): Promise<boolean>;

	function syscall(
		syscall: "host_get_tty_size",
		value: undefined,
	): tg.Process.Tty.Size | undefined;

	function syscall(
		syscall: "host_get_xattr",
		path: string,
		name: string,
	): Promise<Uint8Array | undefined>;

	function syscall(syscall: "host_is_tty", fd: number): boolean;

	function syscall(
		syscall: "host_magic",
		value: Function,
	): tg.Command.Data.Executable;

	function syscall(
		syscall: "host_mkdtemp",
		value: undefined,
	): Promise<string>;

	function syscall(
		syscall: "host_read",
		fd: number,
		length: number | undefined,
		stopper: number | undefined,
	): Promise<Uint8Array | undefined>;

	function syscall(syscall: "host_remove", path: string): Promise<void>;

	function syscall(
		syscall: "host_signal",
		pid: number,
		signal: tg.Process.Signal,
	): Promise<void>;

	function syscall(
		syscall: "host_signal_close",
		token: number,
	): Promise<void>;

	function syscall(
		syscall: "host_signal_open",
		signal: tg.Host.Signal,
	): Promise<number>;

	function syscall(
		syscall: "host_signal_read",
		token: number,
	): Promise<boolean>;

	function syscall(
		syscall: "host_sleep",
		duration: number,
		stopper: number | undefined,
	): Promise<void>;

	function syscall(
		syscall: "host_spawn",
		arg: tg.Host.SpawnArg,
	): Promise<tg.Host.SpawnOutput>;

	function syscall(syscall: "host_stop_close", stopper: number): Promise<void>;

	function syscall(
		syscall: "host_stop_open",
		value: undefined,
	): Promise<number>;

	function syscall(syscall: "host_stop_stop", stopper: number): Promise<void>;

	function syscall(
		syscall: "host_wait",
		pid: number,
		stopper: number | undefined,
	): Promise<tg.Host.WaitOutput>;

	function syscall(
		syscall: "host_write",
		fd: number,
		bytes: Uint8Array,
	): Promise<void>;

	function syscall(
		syscall: "host_write_sync",
		fd: number,
		bytes: Uint8Array,
	): void;
}
