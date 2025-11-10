import type * as tg from "./index.ts";

export let handle: Handle = {} as any;

export let setHandle = (newHandle: Handle) => {
	Object.assign(handle, newHandle);
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
	} & tg.Handle.ReadOptions;

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
		network: boolean;
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
				decode: (value: string) => Uint8Array;
				encode: (value: Uint8Array) => string;
			};
			hex: {
				decode: (value: string) => Uint8Array;
				encode: (value: Uint8Array) => string;
			};
			json: {
				decode: (value: string) => unknown;
				encode: (value: unknown) => string;
			};
			toml: {
				decode: (value: string) => unknown;
				encode: (value: unknown) => string;
			};
			utf8: {
				decode: (value: Uint8Array) => string;
				encode: (value: string) => Uint8Array;
			};
			yaml: {
				decode: (value: string) => unknown;
				encode: (value: unknown) => string;
			};
		};

		log(stream: LogStream, string: string): void;

		objectId(object: tg.Object.Data): tg.Object.Id;

		magic(value: Function): tg.Command.Data.Executable;

		sleep(duration: number): Promise<void>;
	};
}
