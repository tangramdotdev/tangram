export type Config = {
	http?: Http;
	pool?: Pool;
	process?: Process;
	reconnect?: Retry;
	retry?: Retry;
	streamRetry?: Retry;
	token?: string;
	url?: string;
	version?: string;
};
export namespace Config {
	export function defaultValue(): Config {
		return {
			http: Http.defaultValue(),
			pool: Pool.defaultValue(),
			process: Process.defaultValue(),
			reconnect: Retry.defaultValue(),
			retry: Retry.defaultValue(),
			streamRetry: Retry.streamDefaultValue(),
		};
	}
}
export type Http = {
	coalescingTargetSize: number;
	connectTimeout: number;
	http2?: Http2;
	requestTimeout: number;
};
export namespace Http {
	export function defaultValue(): Http {
		return {
			coalescingTargetSize: 16 * 1024,
			connectTimeout: 1,
			requestTimeout: 60,
		};
	}
	export function validate(config: Http): void {
		if (
			!Number.isSafeInteger(config.coalescingTargetSize) ||
			config.coalescingTargetSize <= 0
		)
			throw new Error("invalid HTTP coalescing target size");
		if (!Number.isFinite(config.connectTimeout) || config.connectTimeout <= 0)
			throw new Error("invalid HTTP connect timeout");
		if (!Number.isFinite(config.requestTimeout) || config.requestTimeout <= 0)
			throw new Error("invalid HTTP request timeout");
		if (config.http2 !== undefined) {
			Http2.validate(config.http2);
		}
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
			connectionWindowSize: 64 * 1024 * 1024,
			streamWindowSize: 1024 * 1024,
		};
	}
	export function validate(config: Http2): void {
		if (
			!Number.isSafeInteger(config.connectionWindowSize) ||
			config.connectionWindowSize < 65535 ||
			config.connectionWindowSize > 0x7fffffff
		)
			throw new Error(
				"the HTTP/2 connection window must be between 65535 and 2147483647 bytes",
			);
		if (
			!Number.isSafeInteger(config.streamWindowSize) ||
			config.streamWindowSize < 1 ||
			config.streamWindowSize > 0x7fffffff
		)
			throw new Error(
				"the HTTP/2 stream window must be between 1 and 2147483647 bytes",
			);
		if (
			config.maxConcurrentStreams !== undefined &&
			(!Number.isSafeInteger(config.maxConcurrentStreams) ||
				config.maxConcurrentStreams < 1 ||
				config.maxConcurrentStreams > 0xffffffff)
		)
			throw new Error(
				"the HTTP/2 max concurrent streams must be greater than zero",
			);
		if (config.connectionWindowSize < config.streamWindowSize * 2)
			throw new Error(
				"the HTTP/2 connection window must be at least twice the stream window",
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
export type Process = { connect: ProcessConnect };
export namespace Process {
	export function defaultValue(): Process {
		return { connect: ProcessConnect.defaultValue() };
	}
	export function validate(config: Process): void {
		ProcessConnect.validate(config.connect);
	}
}
export type ProcessConnect = { maxMessages: number; maxRequests: number };
export namespace ProcessConnect {
	export function defaultValue(): ProcessConnect {
		return { maxMessages: 128, maxRequests: 64 };
	}
	export function validate(config: ProcessConnect): void {
		if (!Number.isSafeInteger(config.maxMessages) || config.maxMessages <= 0)
			throw new Error("invalid process connect max messages");
		if (!Number.isSafeInteger(config.maxRequests) || config.maxRequests <= 0)
			throw new Error("invalid process connect max requests");
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
	export function streamDefaultValue(): Retry {
		return {
			backoff: 0.01,
			jitter: 0.01,
			maxDelay: 1,
			maxRetries: Number.MAX_SAFE_INTEGER,
		};
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
export const compatibilityDate = "2026-01-01T00:00:00Z";
export const version = "0.0.0";
