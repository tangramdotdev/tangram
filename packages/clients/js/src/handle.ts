import type * as tg from "./index.ts";

export let handle: Handle = {} as any;

export let setHandle = (newHandle: Handle) => {
	Object.assign(handle, newHandle);
};

export type Handle = {
	checkin(arg: tg.Handle.CheckinArg): Promise<tg.Artifact.Id>;

	checkout(arg: tg.Handle.CheckoutArg): Promise<string>;

	read(arg: tg.Handle.ReadArg): Promise<Uint8Array>;

	write(bytes: string | Uint8Array): Promise<tg.Blob.Id>;
} & tg.Handle.Object &
	tg.Handle.Process &
	tg.Handle.Sandbox &
	tg.Handle.System;

export namespace Handle {
	export type SandboxArg = {
		hostname?: string | undefined;
		mounts?: Array<tg.Sandbox.Mount.Data> | undefined;
		network: boolean;
		ttl?: number | undefined;
		user?: string | undefined;
	};

	export type SandboxGetOutput = {
		hostname?: string | undefined;
		id: tg.Sandbox.Id;
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
		checksum: tg.Checksum | undefined;
		command: tg.Referent<tg.Command.Id>;
		parent: tg.Process.Id | undefined;
		remote: string | undefined;
		retry: boolean;
		sandbox: tg.Handle.SandboxArg | string | undefined;
		stderr: string;
		stdin: string;
		stdout: string;
		tty: boolean | tg.Process.Tty | undefined;
	};

	export type SpawnOutput = {
		cached: boolean;
		process: tg.Process.Id;
		remote: string | undefined;
		token: string | undefined;
		wait: tg.Process.Wait.Data | undefined;
	};

	export type WaitArg = {
		local: boolean | undefined;
		remotes: Array<string> | undefined;
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
		local?: boolean | undefined;
		remotes?: Array<string> | undefined;
		signal: tg.Process.Signal;
	};

	export type ProcessStdioReadArg = {
		length?: number | undefined;
		local?: boolean | undefined;
		position?: number | string | undefined;
		remotes?: Array<string> | undefined;
		size?: number | undefined;
		streams: Array<tg.Process.Stdio.Stream>;
	};

	export type ProcessStdioWriteArg = {
		local?: boolean | undefined;
		remotes?: Array<string> | undefined;
		streams: Array<tg.Process.Stdio.Stream>;
	};

	export type ProcessTtySizePutArg = {
		local?: boolean | undefined;
		remotes?: Array<string> | undefined;
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
			remote: string | undefined,
		): Promise<tg.Process.Data>;

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
		getSandbox(
			id: tg.Sandbox.Id,
			remote: string | undefined,
		): Promise<tg.Handle.SandboxGetOutput>;
	};

	export type System = {
		checksum(
			input: string | Uint8Array,
			algorithm: tg.Checksum.Algorithm,
		): tg.Checksum;

		objectId(object: tg.Object.Data): tg.Object.Id;

		parseValue(value: string): tg.Value.Data;

		processId(): tg.Process.Id;

		stringifyValue(value: tg.Value.Data): string;
	};
}
