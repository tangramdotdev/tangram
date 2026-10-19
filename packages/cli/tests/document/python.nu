use ../lib/test.nu *

# Documenting a Python package produces documentation JSON that matches the snapshot.

let local = server spawn

let path = artifact {
	'tangram.py': '
		"""Utilities for greeting people."""

		from enum import Enum
		from typing import Final, Literal, Optional, TypeAlias, overload

		from . import shapes as shapes
		from .helpers import shout as shout

		VERSION: Final = "1.0"
		"""The version of the package."""

		_cache: dict[str, str] = {}

		Mode: TypeAlias = Literal["quiet", "loud"]

		type Pair[T] = tuple[T, T]


		class Color(Enum):
		    """A color."""

		    RED = 1
		    """The color red."""
		    GREEN = "green"


		def greet(name: str, times: int = 1, *, mode: Mode = "quiet") -> str:
		    """Greet someone.

		    Args:
		        name: The name to greet.
		        times (int): The number of times to greet.

		    Returns:
		        The greeting.
		    """
		    return name * times


		def count(*values: int, **options: str) -> int:
		    """Count the values.

		    :param values: The values to count.
		    :returns: The number of values.
		    """
		    return len(values)


		@overload
		def parse(value: str) -> int: ...
		@overload
		def parse(value: bytes) -> int: ...
		def parse(value):
		    return int(value)


		def infer(value: int):
		    return [value]


		async def build(source: tg.Directory) -> Optional[tg.File]:
		    """Build the source."""
		    return None


		class Greeter:
		    """A greeter.

		    Parameters
		    ----------
		    prefix : str
		        The prefix of each greeting.
		    """

		    default_prefix: str = "Hello"
		    """The default prefix."""

		    def __init__(self, prefix: str = "Hello") -> None:
		        self._prefix = prefix

		    @property
		    def prefix(self) -> str:
		        """The prefix."""
		        return self._prefix

		    def greet(self, name: str) -> str:
		        """Greet someone."""
		        return f"{self._prefix}, {name}!"

		    @classmethod
		    def create(cls) -> "Greeter":
		        return cls()

		    @staticmethod
		    def separator() -> str:
		        return ","

		    def _helper(self) -> None:
		        pass


		def _private() -> None:
		    pass
	'
	'helpers.tg.py': '
		__all__ = ["shout"]


		def shout(text: str) -> str:
		    """Shout the text."""
		    return text.upper()


		def whisper(text: str) -> str:
		    return text.lower()
	'
	'shapes.tg.py': '
		"""Shapes."""


		class Circle:
		    radius: float
	'
}

# Document.
let output = do { cd $path; tg document } | complete
success $output

let json = $output.stdout | from json
snapshot --normalize ($json | to json --indent 2)
