import type * as tg from "./index.ts";

export let host = {} as Host;

export let setHost = (newHost: Host) => {
	Object.defineProperties(host, Object.getOwnPropertyDescriptors(newHost));
};

if (typeof process !== "undefined" && process.versions?.node !== undefined) {
	let { host: nodeHost } = await import("./host/node.ts");
	setHost(nodeHost);
}

export type Host = {
	http2: tg.Host.Http2;

	checksum(
		input: string | Uint8Array,
		algorithm: tg.Checksum.Algorithm,
	): tg.Checksum;

	close(fd: number): Promise<void>;

	current: string;

	disableRawMode(fd: number): Promise<void>;

	enableRawMode(fd: number): Promise<void>;

	exec(arg: tg.Host.SpawnArg): Promise<never>;

	exists(path: string): Promise<boolean>;

	getTtySize(): tg.Process.Tty.Size | null;

	getxattr(path: string, name: string): Promise<Uint8Array | null>;

	isForegroundControllingTty(fd: number): boolean;

	isTty(fd: number): boolean;

	listenSignal(signal: tg.Host.Signal): tg.Host.SignalListener;

	magic(value: Function): tg.Command.Data.Executable;

	mkdtemp(): Promise<string>;

	objectId(object: tg.Object.Data): tg.Object.Id;

	/** The host's available parallelism. */
	parallelism: number;

	parseValue(value: string): tg.Value.Data;

	read(
		fd: number,
		length?: number | null,
		stopper?: tg.Host.Stopper | null,
	): Promise<Uint8Array | null>;

	remove(path: string): Promise<void>;

	signal(pid: number, signal: tg.Process.Signal): Promise<void>;

	sleep(duration: number, stopper?: tg.Host.Stopper | null): Promise<void>;

	stringifyValue(value: tg.Value.Data): string;

	spawn(arg: tg.Host.SpawnArg): Promise<tg.Host.SpawnOutput>;

	stopperClose(stopper: tg.Host.Stopper): Promise<void>;

	stopperOpen(): Promise<tg.Host.Stopper>;

	stopperStop(stopper: tg.Host.Stopper): Promise<void>;

	wait(
		pid: number,
		stopper?: tg.Host.Stopper | null,
	): Promise<tg.Host.WaitOutput>;

	write(fd: number, bytes: Uint8Array): Promise<void>;

	writeSync(fd: number, bytes: Uint8Array): void;
};

export namespace Host {
	export type Http2 = {
		ClientHttp2Session: {
			new (
				authority: string,
				options?: Http2.ConnectOptions,
			): Http2.ClientHttp2Session;
		};
		ClientHttp2Stream: {
			new (
				session: any,
				headers: Http2.Headers,
				options: Http2.RequestOptions,
			): Http2.ClientHttp2Stream;
		};
		connect(
			authority: string | { toString(): string },
			options?:
				| Http2.ConnectOptions
				| ((session: Http2.ClientHttp2Session) => void),
			listener?: (session: Http2.ClientHttp2Session) => void,
		): Http2.ClientHttp2Session;
		constants: {
			HTTP2_HEADER_AUTHORITY: ":authority";
			HTTP2_HEADER_METHOD: ":method";
			HTTP2_HEADER_PATH: ":path";
			HTTP2_HEADER_SCHEME: ":scheme";
			HTTP2_HEADER_STATUS: ":status";
			HTTP2_METHOD_GET: "GET";
			HTTP2_METHOD_POST: "POST";
		};
	};

	export namespace Http2 {
		export type Headers = Record<string, string | number | string[]>;

		export type ConnectOptions = {
			port?: number;
		};

		export type RequestOptions = {
			endStream?: boolean;
		};

		export interface ClientHttp2Session {
			readonly authority: string;
			readonly closed: boolean;
			readonly destroyed: boolean;
			readonly options: ConnectOptions;

			close(callback?: () => void): Promise<void>;
			destroy(error?: Error): void;
			on(event: string, listener: (...args: unknown[]) => void): this;
			once(event: string, listener: (...args: unknown[]) => void): this;
			off(event: string, listener: (...args: unknown[]) => void): this;
			request(headers: Headers, options?: RequestOptions): ClientHttp2Stream;
		}

		export interface ClientHttp2Stream {
			readonly session: ClientHttp2Session;

			close(): this;
			destroy(error?: Error): this;
			end(bytes?: string | Uint8Array): this;
			on(event: string, listener: (...args: unknown[]) => void): this;
			once(event: string, listener: (...args: unknown[]) => void): this;
			off(event: string, listener: (...args: unknown[]) => void): this;
			setEncoding(encoding: "utf8" | "utf-8"): this;
			write(bytes: string | Uint8Array): boolean;
		}
	}

	export type Signal = "sigwinch";

	export type Stopper = number;

	export type Stdio = "inherit" | "null" | "pipe";

	export type SignalListener = AsyncIterable<void> & {
		close(): Promise<void>;
	};

	export type SpawnArg = {
		args: Array<string>;
		cwd: string | null;
		env: { [key: string]: string };
		executable: string;
		stderr: tg.Host.Stdio;
		stdin: tg.Host.Stdio;
		stdout: tg.Host.Stdio;
	};

	export type SpawnOutput = {
		pid: number;
		stderr: number | null;
		stdin: number | null;
		stdout: number | null;
	};

	export type WaitOutput = {
		exit: number;
	};
}
