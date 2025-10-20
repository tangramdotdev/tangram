import type * as tg from "./index.ts";

export let handle: Handle;

export let setHandle = (newHandle: Handle) => {
	handle = newHandle;
};

export type Handle = {
	read(arg: tg.Handle.ReadArg): Promise<Uint8Array>;

	sync(items: Array<tg.Handle.SyncItem>): Promise<void>;

	write(bytes: string | Uint8Array): Promise<tg.Blob.Id>;
} & tg.Handle.Object &
	tg.Handle.Process &
	tg.Handle.System;

export namespace Handle {
	export type ReadArg = {
		blob: tg.Blob.Id;
		options: tg.Handle.ReadOptions;
	};

	export type ReadOptions = {
		position?: number | string | undefined;
		length?: number | undefined;
		size?: number | undefined;
	};

	export type SpawnArg = {
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

	export type SpawnOutput = {
		process: tg.Process.Id;
		remote: string | undefined;
	};

	export type SyncItem = {
		id: tg.Object.Id;
		data: tg.Object.Data;
	};

	export type Object = {
		getObject(id: tg.Object.Id): Promise<tg.Object.Data>;
	};

	export type Process = {
		getProcess(
			id: tg.Process.Id,
			remote: string | undefined,
		): Promise<tg.Process.Data>;

		spawnProcess(arg: SpawnArg): Promise<SpawnOutput>;

		waitProcess(
			id: tg.Process.Id,
			remote: string | undefined,
		): Promise<tg.Process.Wait.Data>;
	};

	export type LogStream = "stdout" | "stderr";

	export type System = {
		checksum(
			input: string | Uint8Array,
			algorithm: tg.Checksum.Algorithm,
		): tg.Checksum;

		encoding: {
			base64: {
				encode: (value: Uint8Array) => string;
				decode: (value: string) => Uint8Array;
			};
			hex: {
				encode: (value: Uint8Array) => string;
				decode: (value: string) => Uint8Array;
			};
			json: {
				encode: (value: unknown) => string;
				decode: (value: string) => unknown;
			};
			toml: {
				encode: (value: unknown) => string;
				decode: (value: string) => unknown;
			};
			utf8: {
				encode: (value: string) => Uint8Array;
				decode: (value: Uint8Array) => string;
			};
			yaml: {
				encode: (value: unknown) => string;
				decode: (value: string) => unknown;
			};
		};

		log(stream: LogStream, string: string): void;

		objectId(object: tg.Object.Data): tg.Object.Id;

		magic(value: Function): tg.Command.Data.Executable;

		sleep(duration: number): Promise<void>;
	};
}
