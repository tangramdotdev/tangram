"""Process stdio queue capacity."""

from ...config import StdioConfig


def capacity(config: StdioConfig) -> int:
    return config["limits"]["messages"] * 2 + 4
