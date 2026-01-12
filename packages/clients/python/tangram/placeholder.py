"""Placeholder class for template interpolation."""

from __future__ import annotations

from typing import Any, Dict


class Placeholder:
    """A named placeholder for template interpolation."""

    _name: str

    def __init__(self, name: str) -> None:
        """Initialize a placeholder with a name."""
        self._name = name

    @property
    def name(self) -> str:
        """Get the placeholder name."""
        return self._name

    def to_data(self) -> Dict[str, Any]:
        """Convert to serializable data."""
        return {"name": self._name}

    @staticmethod
    def from_data(data: Dict[str, Any]) -> Placeholder:
        """Create a placeholder from serialized data."""
        return Placeholder(data["name"])

    def __repr__(self) -> str:
        """Return a string representation."""
        return f"Placeholder({self._name!r})"

    def __eq__(self, other: object) -> bool:
        """Check equality."""
        if not isinstance(other, Placeholder):
            return NotImplemented
        return self._name == other._name

    def __hash__(self) -> int:
        """Return a hash."""
        return hash(self._name)


def placeholder(name: str) -> Placeholder:
    """Create a placeholder with the given name."""
    return Placeholder(name)


# The output placeholder.
output = Placeholder("output")
