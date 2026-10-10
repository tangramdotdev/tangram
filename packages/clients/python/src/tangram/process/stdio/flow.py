"""Process stdio queue capacity."""

from ...config import Config


def capacity(config: Config) -> int:
    return config["limits"]["messages"] * 2 + 4
