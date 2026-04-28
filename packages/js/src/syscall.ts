import type * as tg from "@tangramdotdev/client";

type LocationWire = string;

type ProcessGetArgWire = {
	location?: LocationWire | undefined;
};

type ProcessGetOutputWire = Omit<tg.Handle.ProcessGetOutput, "location"> & {
	location?: LocationWire | undefined;
};

type SandboxGetOutputWire = Omit<tg.Handle.SandboxGetOutput, "location"> & {
	location?: LocationWire | undefined;
};

type SignalArgWire = Omit<tg.Handle.SignalArg, "location"> & {
	location?: LocationWire | undefined;
};

type SpawnArgWire = Omit<
	tg.Handle.SpawnArg,
	"cache_location" | "command" | "location"
> & {
	cache_location?: LocationWire | undefined;
	command: Omit<tg.Referent<tg.Command.Id>, "options"> & {
		options?: tg.Referent.Data.Options | undefined;
	};
	location?: LocationWire | undefined;
};

type SpawnOutputWire = Omit<tg.Handle.SpawnOutput, "location"> & {
	location?: LocationWire | undefined;
};

type WaitArgWire = Omit<tg.Handle.WaitArg, "location"> & {
	location?: LocationWire | undefined;
};

type ProcessStdioReadArgWire = Omit<tg.Handle.ProcessStdioReadArg, "location"> & {
	location?: LocationWire | undefined;
};

type ProcessStdioWriteArgWire = Omit<
	tg.Handle.ProcessStdioWriteArg,
	"location"
> & {
	location?: LocationWire | undefined;
};

type ProcessTtySizePutArgWire = Omit<
	tg.Handle.ProcessTtySizePutArg,
	"location"
> & {
	location?: LocationWire | undefined;
};

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

	function syscall(syscall: "handle_arg"): tg.Handle.Arg;

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

	function syscall(
		syscall: "handle_object_id",
		object: tg.Object.Data,
	): tg.Object.Id;

	function syscall(
		syscall: "handle_process_get",
		id: tg.Process.Id,
		arg: ProcessGetArgWire | undefined,
	): Promise<ProcessGetOutputWire>;

	function syscall(
		syscall: "handle_sandbox_get",
		id: string,
	): Promise<SandboxGetOutputWire>;

	function syscall(
		syscall: "handle_process_signal",
		id: tg.Process.Id,
		arg: SignalArgWire,
	): Promise<void>;

	function syscall(
		syscall: "handle_process_spawn",
		arg: SpawnArgWire,
	): Promise<SpawnOutputWire>;

	function syscall(
		syscall: "handle_process_stdio_read_close",
		token: number,
	): Promise<void>;

	function syscall(
		syscall: "handle_process_stdio_read_open",
		id: tg.Process.Id,
		arg: ProcessStdioReadArgWire,
	): Promise<number | undefined>;

	function syscall(
		syscall: "handle_process_stdio_read_read",
		token: number,
	): Promise<tg.Process.Stdio.Read.Event.Data | undefined>;

	function syscall(
		syscall: "handle_process_stdio_write_close",
		token: number,
	): Promise<void>;

	function syscall(
		syscall: "handle_process_stdio_write_open",
		id: tg.Process.Id,
		arg: ProcessStdioWriteArgWire,
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
		arg: ProcessTtySizePutArgWire,
	): Promise<void>;

	function syscall(
		syscall: "handle_process_wait",
		id: tg.Process.Id,
		arg: WaitArgWire,
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

	function syscall(syscall: "host_current"): string | undefined;

	function syscall(syscall: "host_disable_raw_mode", fd: number): Promise<void>;

	function syscall(syscall: "host_enable_raw_mode", fd: number): Promise<void>;

	function syscall(
		syscall: "host_exec",
		arg: tg.Host.SpawnArg,
	): Promise<never>;

	function syscall(syscall: "host_exists", path: string): Promise<boolean>;

	function syscall(syscall: "host_get_tty_size"): tg.Process.Tty.Size | undefined;

	function syscall(
		syscall: "host_get_xattr",
		path: string,
		name: string,
	): Promise<Uint8Array | undefined>;

	function syscall(syscall: "host_is_foreground_controlling_tty", fd: number): boolean;

	function syscall(syscall: "host_is_tty", fd: number): boolean;

	function syscall(
		syscall: "host_magic",
		value: Function,
	): tg.Command.Data.Executable;

	function syscall(syscall: "host_mkdtemp"): Promise<string>;

	function syscall(syscall: "host_parallelism"): number;

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
		stopper: number | undefined,
	): Promise<void>;

	function syscall(
		syscall: "host_spawn",
		arg: tg.Host.SpawnArg,
	): Promise<tg.Host.SpawnOutput>;

	function syscall(syscall: "host_stopper_close", stopper: number): Promise<void>;

	function syscall(syscall: "host_stopper_open"): Promise<number>;

	function syscall(syscall: "host_stopper_stop", stopper: number): Promise<void>;

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
