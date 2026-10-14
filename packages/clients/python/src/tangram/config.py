"""Tangram client configuration and defaults."""

from __future__ import annotations

import math
from typing import NotRequired, TypedDict


class Config(TypedDict, total=False):
    http: HttpConfig
    pool: PoolOptions
    process: ProcessConfig
    reconnect: RetryOptions
    retry: RetryOptions
    stream_retry: RetryOptions
    token: str
    url: str
    version: str


class HttpConfig(TypedDict):
    coalescing_target_size: int
    connect_timeout: float
    http2: NotRequired[Http2Config]
    request_timeout: float


class Http2Config(TypedDict):
    connection_window_size: int
    max_concurrent_streams: NotRequired[int | None]
    stream_window_size: int


class PoolOptions(TypedDict):
    max: int
    min: int
    shared: int
    ttl: float | None


class ProcessConfig(TypedDict):
    connect: ProcessConnectConfig


class ProcessConnectConfig(TypedDict):
    max_messages: int
    max_requests: int


class RetryOptions(TypedDict):
    backoff: float
    jitter: float
    max_delay: float
    max_retries: int


def default_config() -> Config:
    return {
        "http": default_http_config(),
        "pool": default_pool_options(),
        "process": default_process_config(),
        "reconnect": default_retry_options(),
        "retry": default_retry_options(),
        "stream_retry": default_stream_retry_options(),
    }


def default_http_config() -> HttpConfig:
    return {
        "coalescing_target_size": 16 * 1024,
        "connect_timeout": 1,
        "request_timeout": 60,
    }


def default_http2_config() -> Http2Config:
    return {
        "connection_window_size": 64 * 1024 * 1024,
        "max_concurrent_streams": None,
        "stream_window_size": 1024 * 1024,
    }


def default_pool_options() -> PoolOptions:
    return {"max": 1, "min": 0, "shared": 2**53 - 1, "ttl": None}


def default_process_config() -> ProcessConfig:
    return {"connect": default_process_connect_config()}


def default_process_connect_config() -> ProcessConnectConfig:
    return {"max_messages": 128, "max_requests": 64}


def default_retry_options() -> RetryOptions:
    return {"backoff": 0.01, "jitter": 0.01, "max_delay": 1, "max_retries": 3}


def default_stream_retry_options() -> RetryOptions:
    return {
        "backoff": 0.01,
        "jitter": 0.01,
        "max_delay": 1,
        "max_retries": 2**53 - 1,
    }


def validate_http_config(config: HttpConfig) -> None:
    size = config["coalescing_target_size"]
    if type(size) is not int or not 0 < size <= 2**53 - 1:
        raise ValueError("invalid HTTP coalescing target size")
    if not is_positive_duration(config["connect_timeout"]):
        raise ValueError("invalid HTTP connect timeout")
    if not is_positive_duration(config["request_timeout"]):
        raise ValueError("invalid HTTP request timeout")
    if "http2" in config:
        validate_http2_config(config["http2"])


def validate_http2_config(config: Http2Config) -> None:
    connection_window_size = config["connection_window_size"]
    if (
        type(connection_window_size) is not int
        or not 65535 <= connection_window_size <= 0x7FFFFFFF
    ):
        raise ValueError(
            "the HTTP/2 connection window must be between 65535 and 2147483647 bytes"
        )
    stream_window_size = config["stream_window_size"]
    if type(stream_window_size) is not int or not 1 <= stream_window_size <= 0x7FFFFFFF:
        raise ValueError(
            "the HTTP/2 stream window must be between 1 and 2147483647 bytes"
        )
    max_concurrent_streams = config.get("max_concurrent_streams")
    if max_concurrent_streams is not None and (
        type(max_concurrent_streams) is not int
        or not 1 <= max_concurrent_streams <= 0xFFFFFFFF
    ):
        raise ValueError("the HTTP/2 max concurrent streams must be greater than zero")
    if connection_window_size < stream_window_size * 2:
        raise ValueError(
            "the HTTP/2 connection window must be at least twice the stream window"
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


def validate_process_config(config: ProcessConfig) -> None:
    validate_process_connect_config(config["connect"])


def validate_process_connect_config(config: ProcessConnectConfig) -> None:
    for name in ("max_messages", "max_requests"):
        value = config[name]
        if type(value) is not int or not 0 < value <= 2**53 - 1:
            raise ValueError(f"invalid process connect {name.replace('_', ' ')}")


def validate_retry_options(options: RetryOptions) -> None:
    for value in (options["backoff"], options["jitter"], options["max_delay"]):
        if not math.isfinite(value) or value < 0:
            raise ValueError("invalid retry duration")
    if type(options["max_retries"]) is not int or options["max_retries"] < 0:
        raise ValueError("invalid retry count")


def is_positive_duration(value: float) -> bool:
    return type(value) in (int, float) and math.isfinite(value) and value > 0


compatibility_date = "2026-01-01T00:00:00Z"


version = "0.0.0"
