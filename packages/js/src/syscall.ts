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
		syscall: "host_checksum",
		input: string | Uint8Array,
		algorithm: tg.Checksum.Algorithm,
	): tg.Checksum;

	function syscall(syscall: "host_close", fd: number): Promise<void>;

	function syscall(syscall: "host_current"): string;

	function syscall(syscall: "host_disable_raw_mode", fd: number): Promise<void>;

	function syscall(syscall: "host_enable_raw_mode", fd: number): Promise<void>;

	function syscall(syscall: "host_exec", arg: tg.Host.SpawnArg): Promise<never>;

	function syscall(syscall: "host_exists", path: string): Promise<boolean>;

	function syscall(
		syscall: "host_get_tty_size",
	): tg.Process.Tty.Size | null;

	function syscall(
		syscall: "host_get_xattr",
		path: string,
		name: string,
	): Promise<Uint8Array | null>;

	function syscall(
		syscall: "host_is_foreground_controlling_tty",
		fd: number,
	): boolean;

	function syscall(syscall: "host_is_tty", fd: number): boolean;

	function syscall(
		syscall: "host_magic",
		value: Function,
	): tg.Command.Data.Executable;

	function syscall(syscall: "host_mkdtemp"): Promise<string>;

	function syscall(syscall: "host_object_id", object: tg.Object.Data): tg.Object.Id;

	function syscall(syscall: "host_parallelism"): number;

	function syscall(syscall: "host_value_parse", value: string): tg.Value.Data;

	function syscall(syscall: "host_value_stringify", value: tg.Value.Data): string;

	function syscall(
		syscall: "host_read",
		fd: number,
		length: number | null,
		stopper: number | null,
	): Promise<Uint8Array | null>;

	function syscall(syscall: "host_remove", path: string): Promise<void>;

	function syscall(
		syscall: "host_signal",
		pid: number,
		signal: tg.Process.Signal,
	): Promise<void>;

	function syscall(syscall: "host_signal_close", token: number): Promise<void>;

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
		stopper: number | null,
	): Promise<void>;

	function syscall(
		syscall: "host_spawn",
		arg: tg.Host.SpawnArg,
	): Promise<tg.Host.SpawnOutput>;

	function syscall(
		syscall: "host_stopper_close",
		stopper: number,
	): Promise<void>;

	function syscall(syscall: "host_stopper_open"): Promise<number>;

	function syscall(
		syscall: "host_stopper_stop",
		stopper: number,
	): Promise<void>;

	function syscall(
		syscall: "host_wait",
		pid: number,
		stopper: number | null,
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

	function syscall(
		syscall: "http2_connect",
		authority: string,
		options: { port?: number },
	): Promise<number>;

	function syscall(syscall: "http2_session_close", session: number): Promise<void>;

	function syscall(
		syscall: "http2_session_destroy",
		session: number,
		error: string | null,
	): Promise<void>;

	function syscall(
		syscall: "http2_session_request",
		session: number,
		headers: [string, string][],
		options: { end_stream: boolean },
	): Promise<number>;

	function syscall(syscall: "http2_stream_close", stream: number): Promise<void>;

	function syscall(
		syscall: "http2_stream_end",
		stream: number,
		bytes: Uint8Array | null,
	): Promise<void>;

	function syscall(
		syscall: "http2_stream_read",
		stream: number,
	): Promise<unknown | null>;

	function syscall(
		syscall: "http2_stream_write",
		stream: number,
		bytes: Uint8Array,
	): Promise<void>;
}
