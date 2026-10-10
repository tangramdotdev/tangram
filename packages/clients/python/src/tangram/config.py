"""Tangram client configuration and defaults."""

from __future__ import annotations

import math
from typing import TypedDict

from .http.flow import Limits, validate_limits


class Config(TypedDict, total=False):
    http: HttpConfig
    pool: PoolOptions
    reconnect: RetryOptions
    retry: RetryOptions
    stdio: StdioConfig
    sync: SyncConfig
    token: str
    url: str
    version: str


class HttpConfig(TypedDict):
    coalescing_target_size: int
    http2: Http2Config


class Http2Config(TypedDict):
    connection_window_size: int
    max_concurrent_streams: int | None
    stream_window_size: int


class PoolOptions(TypedDict):
    max: int
    min: int
    shared: int
    ttl: float | None


class RetryOptions(TypedDict):
    backoff: float
    jitter: float
    max_delay: float
    max_retries: int


class StdioConfig(TypedDict):
    limits: Limits
    max_message_size: int
    max_reads: int


class SyncConfig(TypedDict):
    limits: Limits
    max_frame_size: int
    max_message_size: int
    max_object_size: int


def default_config() -> Config:
    return {
        "http": default_http_config(),
        "pool": default_pool_options(),
        "reconnect": default_retry_options(),
        "retry": default_retry_options(),
        "stdio": default_stdio_config(),
        "sync": default_sync_config(),
    }


def default_http_config() -> HttpConfig:
    return {"coalescing_target_size": 16 * 1024, "http2": default_http2_config()}


def default_http2_config() -> Http2Config:
    return {
        "connection_window_size": 1024 * 1024 * 1024,
        "max_concurrent_streams": None,
        "stream_window_size": 64 * 1024 * 1024,
    }


def default_pool_options() -> PoolOptions:
    return {"max": 1, "min": 0, "shared": 2**53 - 1, "ttl": None}


def default_retry_options() -> RetryOptions:
    return {"backoff": 0.01, "jitter": 0.01, "max_delay": 1, "max_retries": 3}


def default_stdio_config() -> StdioConfig:
    return {
        "limits": {"bytes": 2 * 1024 * 1024, "messages": 64},
        "max_message_size": 32 * 1024,
        "max_reads": 4,
    }


def default_sync_config() -> SyncConfig:
    return {
        "limits": {"bytes": 2 * 1024 * 1024, "messages": 1024},
        "max_frame_size": 64 * 1024 * 1024,
        "max_message_size": 512 * 1024,
        "max_object_size": 256 * 1024,
    }


def validate_http_config(
    config: HttpConfig, stdio: StdioConfig, sync: SyncConfig
) -> None:
    validate_http2_config(config["http2"], stdio, sync)
    size = config["coalescing_target_size"]
    if type(size) is not int or not 0 < size <= 2**53 - 1:
        raise ValueError("invalid HTTP coalescing target size")


def validate_http2_config(
    config: Http2Config,
    stdio: StdioConfig | None = None,
    sync: SyncConfig | None = None,
) -> None:
    for size in (config["connection_window_size"], config["stream_window_size"]):
        if (
            not isinstance(size, int)
            or isinstance(size, bool)
            or not 0 < size <= 0x7FFFFFFF
        ):
            raise ValueError("invalid HTTP/2 window size")
    limit = config["max_concurrent_streams"]
    if limit is not None and (
        not isinstance(limit, int)
        or isinstance(limit, bool)
        or not 0 < limit <= 0xFFFFFFFF
    ):
        raise ValueError("invalid HTTP/2 stream limit")
    if config["connection_window_size"] < 65535:
        raise ValueError("the HTTP/2 connection window must be at least 65535 bytes")
    if config["connection_window_size"] < config["stream_window_size"] * 2:
        raise ValueError(
            "the HTTP/2 connection window must be at least twice the stream window"
        )
    if stdio is not None:
        minimum = (
            (stdio["limits"]["bytes"] + stdio["limits"]["messages"] * 512)
            * (stdio["max_reads"] + 1)
            + (sync if sync is not None else default_sync_config())["limits"]["bytes"]
        ) * 4
        if config["stream_window_size"] < minimum:
            raise ValueError(
                "the HTTP/2 stream window must leave headroom "
                "for the stdio and sync windows"
            )


def validate_pool_options(options: PoolOptions) -> None:
    for name in ("max", "min", "shared"):
        if type(options[name]) is not int or not 0 <= options[name] <= 2**53 - 1:
            raise ValueError("invalid pool options")
    if options["max"] == 0 or options["shared"] == 0 or options["min"] > options["max"]:
        raise ValueError("invalid pool options")
    ttl = options["ttl"]
    if ttl is not None and (not math.isfinite(ttl) or ttl < 0):
        raise ValueError("invalid pool TTL")


def validate_retry_options(options: RetryOptions) -> None:
    for value in (options["backoff"], options["jitter"], options["max_delay"]):
        if not math.isfinite(value) or value < 0:
            raise ValueError("invalid retry duration")
    if type(options["max_retries"]) is not int or options["max_retries"] < 0:
        raise ValueError("invalid retry count")


def validate_stdio_config(config: StdioConfig) -> None:
    validate_limits(config["limits"])
    for value in (
        *config["limits"].values(),
        config["max_message_size"],
        config["max_reads"],
    ):
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or not 0 < value <= 2**53 - 1
        ):
            raise ValueError("invalid stdio configuration")
    if config["max_message_size"] > config["limits"]["bytes"] // 2:
        raise ValueError("invalid stdio message size")


def validate_stdio_receiver(config: StdioConfig, receiver: StdioConfig) -> None:
    validate_stdio_config(receiver)
    if (
        receiver["limits"]["bytes"] > config["limits"]["bytes"]
        or receiver["limits"]["messages"] > config["limits"]["messages"]
        or receiver["max_message_size"] > config["max_message_size"]
    ):
        raise ValueError("the requested stdio window exceeds the client limits")


def validate_sync_config(config: SyncConfig) -> None:
    validate_limits(config["limits"])
    for value in (
        config["max_frame_size"],
        config["max_message_size"],
        config["max_object_size"],
    ):
        if type(value) is not int or not 0 < value <= 2**53 - 1:
            raise ValueError("invalid sync message limits")
    if (
        config["max_message_size"] < config["max_object_size"] + 256
        or config["max_message_size"] > config["limits"]["bytes"] // 2
        or config["max_message_size"] > config["max_frame_size"]
    ):
        raise ValueError("invalid sync message limits")


compatibility_date = "2026-01-01T00:00:00Z"


version = "0.0.0"
