import type * as tg from "@tangramdotdev/client";

export let handle: tg.Handle = {
	read(arg: tg.Handle.ReadArg): Promise<Uint8Array> {
		return syscall("read", arg);
	},
	sync(items: Array<tg.Handle.SyncItem>): Promise<void> {
		return syscall("sync", items);
	},
	write(bytes: string | Uint8Array): Promise<tg.Blob.Id> {
		return syscall("write", bytes);
	},
	getObject(id) {
		return syscall("object_get", id);
	},
	getProcess(
		id: tg.Process.Id,
		remote: string | undefined,
	): Promise<tg.Process.Data> {
		return syscall("process_get", id, remote);
	},
	spawnProcess(arg: tg.Handle.SpawnArg): Promise<tg.Handle.SpawnOutput> {
		return syscall("process_spawn", arg);
	},
	waitProcess(
		id: tg.Process.Id,
		remote: string | undefined,
	): Promise<tg.Process.Wait.Data> {
		return syscall("process_wait", id, remote);
	},
	checksum(
		input: string | Uint8Array,
		algorithm: tg.Checksum.Algorithm,
	): tg.Checksum {
		return syscall("checksum", input, algorithm);
	},
	encoding: {
		base64: {
			decode(value: string): Uint8Array {
				return syscall("encoding_base64_decode", value);
			},
			encode(value: Uint8Array): string {
				return syscall("encoding_base64_encode", value);
			},
		},
		hex: {
			decode(value: string): Uint8Array {
				return syscall("encoding_hex_decode", value);
			},
			encode(value: Uint8Array): string {
				return syscall("encoding_hex_encode", value);
			},
		},
		json: {
			decode(value: string): unknown {
				return syscall("encoding_json_decode", value);
			},
			encode(value: unknown): string {
				return syscall("encoding_json_encode", value);
			},
		},
		toml: {
			decode(value: string): unknown {
				return syscall("encoding_toml_decode", value);
			},
			encode(value: unknown): string {
				return syscall("encoding_toml_encode", value);
			},
		},
		utf8: {
			decode(value: Uint8Array): string {
				return syscall("encoding_utf8_decode", value);
			},
			encode(value: string): Uint8Array {
				return syscall("encoding_utf8_encode", value);
			},
		},
		yaml: {
			decode(value: string): unknown {
				return syscall("encoding_yaml_decode", value);
			},
			encode(value: unknown): string {
				return syscall("encoding_yaml_encode", value);
			},
		},
	},
	log(stream: tg.Handle.LogStream, string: string): void {
		syscall("log", stream, string);
	},
	magic(value: Function): tg.Command.Data.Executable {
		return syscall("magic", value);
	},
	objectId(object: tg.Object.Data): tg.Object.Id {
		return syscall("object_id", object);
	},
	sleep(duration: number): Promise<void> {
		return syscall("sleep", duration);
	},
};
