import type * as tg from "./index.ts";

export let handle: Handle = {} as any;

export let setHandle = (newHandle: Handle) => {
	Object.defineProperties(handle, Object.getOwnPropertyDescriptors(newHandle));
};

export type Handle = {
	arg(): tg.Handle.Arg;

	checkin(arg: tg.Handle.CheckinArg): Promise<tg.Artifact.Id>;

	checkout(arg: tg.Handle.CheckoutArg): Promise<string>;

	read(arg: tg.Handle.ReadArg): Promise<Uint8Array>;

	write(bytes: string | Uint8Array): Promise<tg.Blob.Id>;
} & tg.Handle.Object &
	tg.Handle.Process &
	tg.Handle.Sandbox &
	tg.Handle.System;

export namespace Handle {
	export type Arg = {
		process?: tg.Process.Id | undefined;
		token?: string | undefined;
		url?: string | undefined;
	};

	export type SandboxArg = {
		cpu?: number | undefined;
		hostname?: string | undefined;
		memory?: number | undefined;
		mounts?: Array<tg.Sandbox.Mount.Data> | undefined;
		network: boolean;
		ttl?: number | undefined;
		user?: string | undefined;
	};

	export type SandboxGetOutput = {
		cpu?: number | undefined;
		hostname?: string | undefined;
		id: tg.Sandbox.Id;
		location?: tg.Location | undefined;
		memory?: number | undefined;
		mounts: Array<tg.Sandbox.Mount.Data>;
		network: boolean;
		status: tg.Sandbox.Status;
		ttl?: number | undefined;
		user?: string | undefined;
	};

	export type Lock = "auto" | "attr" | "file";

	export type ReadArg = {
		blob: tg.Blob.Id;
	} & tg.Handle.ReadOptions;

	export type ReadOptions = {
		position?: number | string | undefined;
		length?: number | undefined;
		size?: number | undefined;
	};

	export type SpawnArg = {
		cache_location?: tg.Location.Arg | undefined;
		checksum?: tg.Checksum | undefined;
		command: tg.Referent<tg.Command.Id>;
		location?: tg.Location.Arg | undefined;
		parent?: tg.Process.Id | undefined;
		retry?: boolean | undefined;
		sandbox?: tg.Handle.SandboxArg | string | undefined;
		stderr?: string | undefined;
		stdin?: string | undefined;
		stdout?: string | undefined;
		tty?: boolean | tg.Process.Tty | undefined;
	};

	export type SpawnOutput = {
		cached: boolean;
		location: tg.Location | undefined;
		process: tg.Process.Id;
		token: string | undefined;
		wait: tg.Process.Wait.Data | undefined;
	};

	export type WaitArg = {
		location?: tg.Location.Arg | undefined;
		token: string | undefined;
	};

	export type CheckoutArg = {
		artifact: tg.Artifact.Id;
		dependencies: boolean;
		extension?: string | undefined;
		force: boolean;
		lock?: tg.Handle.Lock | undefined;
		path?: string | undefined;
	};

	export type CheckinArg = {
		options: tg.Handle.CheckinOptions;
		path: string;
		updates: Array<string>;
	};

	export type CheckinOptions = {
		cachePointers?: boolean | undefined;
		destructive?: boolean | undefined;
		deterministic?: boolean | undefined;
		root?: boolean | undefined;
		ignore?: boolean | undefined;
		localDependencies?: boolean | undefined;
		lock?: tg.Handle.Lock | undefined;
		locked?: boolean | undefined;
		solve?: boolean | undefined;
		unsolvedDependencies?: boolean | undefined;
		ttl?: number | undefined;
		watch?: boolean | undefined;
	};

	export type SignalArg = {
		location?: tg.Location.Arg | undefined;
		signal: tg.Process.Signal;
	};

	export type ProcessGetArg = {
		location?: tg.Location.Arg | undefined;
	};

	export type ProcessGetOutput = {
		data: tg.Process.Data;
		id: tg.Process.Id;
		location?: tg.Location | undefined;
		metadata?: unknown;
	};

	export type ProcessStdioReadArg = {
		length?: number | undefined;
		location?: tg.Location.Arg | undefined;
		position?: number | string | undefined;
		size?: number | undefined;
		streams: Array<tg.Process.Stdio.Stream>;
	};

	export type ProcessStdioWriteArg = {
		location?: tg.Location.Arg | undefined;
		streams: Array<tg.Process.Stdio.Stream>;
	};

	export type ProcessTtySizePutArg = {
		location?: tg.Location.Arg | undefined;
		size: tg.Process.Tty.Size;
	};

	export type PostObjectBatchArg = {
		objects: Array<{
			id: tg.Object.Id;
			data: tg.Object.Data;
		}>;
	};

	export type Object = {
		getObject(id: tg.Object.Id): Promise<tg.Object.Data>;
		postObjectBatch(arg: PostObjectBatchArg): Promise<void>;
	};

	export type Process = {
		getProcess(
			id: tg.Process.Id,
			arg?: tg.Handle.ProcessGetArg | undefined,
		): Promise<tg.Handle.ProcessGetOutput>;

		readProcessStdio(
			id: tg.Process.Id,
			arg: tg.Handle.ProcessStdioReadArg,
		): Promise<AsyncIterableIterator<tg.Process.Stdio.Read.Event> | undefined>;

		setProcessTtySize(
			id: tg.Process.Id,
			arg: tg.Handle.ProcessTtySizePutArg,
		): Promise<void>;

		signalProcess(id: tg.Process.Id, arg: SignalArg): Promise<void>;

		spawnProcess(arg: SpawnArg): Promise<SpawnOutput>;

		waitProcess(
			id: tg.Process.Id,
			arg: tg.Handle.WaitArg,
		): Promise<tg.Process.Wait.Data>;

		writeProcessStdio(
			id: tg.Process.Id,
			arg: tg.Handle.ProcessStdioWriteArg,
			input: AsyncIterableIterator<tg.Process.Stdio.Read.Event>,
		): Promise<void>;
	};

	export type Sandbox = {
		getSandbox(id: tg.Sandbox.Id): Promise<tg.Handle.SandboxGetOutput>;
	};

	export type System = {
		checksum(
			input: string | Uint8Array,
			algorithm: tg.Checksum.Algorithm,
		): tg.Checksum;

		objectId(object: tg.Object.Data): tg.Object.Id;

		parseValue(value: string): tg.Value.Data;

		stringifyValue(value: tg.Value.Data): string;
	};
}
