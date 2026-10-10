import { Limits } from "./http/flow.ts";

export type Config = {
	http?: Http;
	pool?: Pool;
	reconnect?: Retry;
	retry?: Retry;
	stdio?: Stdio;
	sync?: Sync;
	token?: string;
	url?: string;
	version?: string;
};
export namespace Config {
	export function defaultValue(): Config {
		return {
			http: Http.defaultValue(),
			pool: Pool.defaultValue(),
			reconnect: Retry.defaultValue(),
			retry: Retry.defaultValue(),
			stdio: Stdio.defaultValue(),
			sync: Sync.defaultValue(),
		};
	}
}
export type Http = { coalescingTargetSize: number; http2: Http2 };
export namespace Http {
	export function defaultValue(): Http {
		return { coalescingTargetSize: 16 * 1024, http2: Http2.defaultValue() };
	}
	export function validate(config: Http, stdio: Stdio, sync: Sync): void {
		Http2.validate(config.http2, stdio, sync);
		if (
			!Number.isSafeInteger(config.coalescingTargetSize) ||
			config.coalescingTargetSize <= 0
		)
			throw new Error("invalid HTTP coalescing target size");
	}
}
export type Http2 = {
	connectionWindowSize: number;
	maxConcurrentStreams?: number;
	streamWindowSize: number;
};

export namespace Http2 {
	export function defaultValue(): Http2 {
		return {
			connectionWindowSize: 1024 * 1024 * 1024,
			streamWindowSize: 64 * 1024 * 1024,
		};
	}

	export function validate(config: Http2, stdio: Stdio, sync: Sync): void {
		for (let size of [config.connectionWindowSize, config.streamWindowSize]) {
			if (!Number.isSafeInteger(size) || size <= 0 || size > 0x7fffffff)
				throw new Error("invalid HTTP/2 window size");
		}
		if (
			config.maxConcurrentStreams !== undefined &&
			(!Number.isSafeInteger(config.maxConcurrentStreams) ||
				config.maxConcurrentStreams <= 0 ||
				config.maxConcurrentStreams > 0xffffffff)
		)
			throw new Error("invalid HTTP/2 stream limit");
		if (config.connectionWindowSize < 65535)
			throw new Error(
				"the HTTP/2 connection window must be at least 65535 bytes",
			);
		if (config.connectionWindowSize < config.streamWindowSize * 2)
			throw new Error(
				"the HTTP/2 connection window must be at least twice the stream window",
			);
		let minimum =
			((stdio.limits.bytes + stdio.limits.messages * 512) *
				(stdio.maxReads + 1) +
				sync.limits.bytes) *
			4;
		if (!Number.isSafeInteger(minimum) || config.streamWindowSize < minimum)
			throw new Error(
				"the HTTP/2 stream window must leave headroom for the stdio and sync windows",
			);
	}
}
export type Pool = { max: number; min: number; shared: number; ttl?: number };
export namespace Pool {
	export function defaultValue(): Pool {
		return { max: 1, min: 0, shared: Number.MAX_SAFE_INTEGER };
	}
	export function validate(config: Pool): void {
		if (
			!Number.isSafeInteger(config.max) ||
			config.max < 1 ||
			!Number.isSafeInteger(config.min) ||
			config.min < 0 ||
			config.min > config.max ||
			!Number.isSafeInteger(config.shared) ||
			config.shared < 1 ||
			(config.ttl !== undefined &&
				(!Number.isFinite(config.ttl) || config.ttl < 0))
		)
			throw new Error("invalid pool options");
	}
}
export type Retry = {
	backoff: number;
	jitter: number;
	maxDelay: number;
	maxRetries: number;
};
export namespace Retry {
	export function defaultValue(): Retry {
		return { backoff: 0.01, jitter: 0.01, maxDelay: 1, maxRetries: 3 };
	}
	export function validate(options: Retry): void {
		for (let value of [options.backoff, options.jitter, options.maxDelay]) {
			if (!Number.isFinite(value) || value < 0)
				throw new Error("invalid retry duration");
		}
		if (!Number.isSafeInteger(options.maxRetries) || options.maxRetries < 0)
			throw new Error("invalid retry count");
	}
}
export type Stdio = {
	limits: Limits;
	maxMessageSize: number;
	maxReads: number;
};

export namespace Stdio {
	export function defaultValue(): Stdio {
		return {
			limits: { bytes: 2 * 1024 * 1024, messages: 64 },
			maxMessageSize: 32 * 1024,
			maxReads: 4,
		};
	}

	export function validate(config: Stdio): void {
		Limits.validate(config.limits);
		for (let value of [
			config.limits.bytes,
			config.limits.messages,
			config.maxMessageSize,
			config.maxReads,
		]) {
			if (!Number.isSafeInteger(value) || value <= 0)
				throw new Error("invalid stdio configuration");
		}
		if (config.maxMessageSize > Math.floor(config.limits.bytes / 2))
			throw new Error("invalid stdio message size");
	}

	export function validateReceiver(config: Stdio, receiver: Stdio): void {
		validate(receiver);
		if (
			receiver.limits.bytes > config.limits.bytes ||
			receiver.limits.messages > config.limits.messages ||
			receiver.maxMessageSize > config.maxMessageSize
		)
			throw new Error("the requested stdio window exceeds the client limits");
	}

	export function toData(config: Stdio) {
		return {
			limits: config.limits,
			max_message_size: config.maxMessageSize,
			max_reads: config.maxReads,
		};
	}
}
export type Sync = {
	limits: Limits;
	maxFrameSize: number;
	maxMessageSize: number;
	maxObjectSize: number;
};
export namespace Sync {
	export function defaultValue(): Sync {
		return {
			limits: { bytes: 2 * 1024 * 1024, messages: 1024 },
			maxFrameSize: 64 * 1024 * 1024,
			maxMessageSize: 512 * 1024,
			maxObjectSize: 256 * 1024,
		};
	}
	export function validate(config: Sync): void {
		Limits.validate(config.limits);
		for (let value of [
			config.maxFrameSize,
			config.maxMessageSize,
			config.maxObjectSize,
		])
			if (!Number.isSafeInteger(value) || value <= 0)
				throw new Error("invalid sync message limits");
		if (
			config.maxMessageSize < config.maxObjectSize + 256 ||
			config.maxMessageSize > Math.floor(config.limits.bytes / 2) ||
			config.maxMessageSize > config.maxFrameSize
		)
			throw new Error("invalid sync message limits");
	}
}
export const compatibilityDate = "2026-01-01T00:00:00Z";
export const version = "0.0.0";
