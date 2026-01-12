"""Template class for string interpolation."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Union

from tangram.placeholder import Placeholder


# Template component: string, placeholder, or artifact reference.
Component = Union[str, Placeholder, Any]  # Any for Artifact types


def unindent(string: str) -> str:
    """Remove common leading whitespace from all lines.

    This matches the JavaScript unindent behavior for template literals.
    """
    if not string:
        return string

    lines = string.split("\n")

    # Skip first line if empty (common in multi-line strings).
    start_idx = 1 if lines and not lines[0].strip() else 0

    # Find minimum indentation across non-empty lines.
    min_indent: Optional[int] = None
    for line in lines[start_idx:]:
        if line.strip():  # Non-empty line.
            match = re.match(r"^(\s*)", line)
            if match:
                indent = len(match.group(1))
                if min_indent is None or indent < min_indent:
                    min_indent = indent

    if min_indent is None or min_indent == 0:
        # No common indentation to remove.
        # Still remove first/last empty lines.
        if lines and not lines[0].strip():
            lines = lines[1:]
        if lines and not lines[-1].strip():
            lines = lines[:-1]
        return "\n".join(lines)

    # Remove the common indentation from each line.
    result_lines = []
    for i, line in enumerate(lines):
        if i == 0 and not line.strip():
            continue  # Skip empty first line.
        if line.strip():  # Non-empty line.
            result_lines.append(line[min_indent:])
        else:
            result_lines.append("")  # Keep empty lines.

    # Remove trailing empty line if present.
    if result_lines and not result_lines[-1].strip():
        result_lines = result_lines[:-1]

    return "\n".join(result_lines)


class Template:
    """String template with placeholders and artifact references."""

    _components: List[Component]

    def __init__(self, components: List[Component]) -> None:
        """Initialize a template with components."""
        self._components = components

    @classmethod
    async def new(cls, *components: Any) -> "Template":
        """Create a new template, resolving all components."""
        from tangram.resolve import resolve

        resolved = await resolve(list(components))
        return template(*resolved)

    @classmethod
    async def join(cls, templates: List["Template"], separator: str = "") -> "Template":
        """Join multiple templates with a separator."""
        if not templates:
            return Template([])
        result_components: List[Component] = []
        for i, t in enumerate(templates):
            if i > 0 and separator:
                result_components.append(separator)
            result_components.extend(t.components)
        return Template(result_components)

    def objects(self) -> List[Any]:
        """Get all object components from this template."""
        from tangram.object_ import Object_

        return [c for c in self._components if isinstance(c, Object_)]

    @property
    def components(self) -> List[Component]:
        """Get the template components."""
        return self._components

    def to_data(self) -> Dict[str, Any]:
        """Convert to serializable data."""
        from tangram.object_ import Object_
        from tangram.value import to_data

        components = []
        for component in self._components:
            if isinstance(component, str):
                components.append(component)
            elif isinstance(component, Placeholder):
                components.append({"kind": "placeholder", "value": component.to_data()})
            elif isinstance(component, Object_):
                components.append({"kind": "artifact", "value": component.id})
            else:
                components.append(to_data(component))
        return {"components": components}

    @staticmethod
    def from_data(data: Dict[str, Any]) -> Template:
        """Create a template from serialized data."""
        from tangram.object_ import Object_
        from tangram.value import from_data

        components: List[Component] = []
        for component in data["components"]:
            if isinstance(component, str):
                components.append(component)
            elif isinstance(component, dict):
                kind = component.get("kind")
                if kind == "placeholder":
                    components.append(Placeholder.from_data(component["value"]))
                elif kind == "artifact":
                    components.append(Object_.with_id(component["value"]))
                else:
                    components.append(from_data(component))
            else:
                components.append(component)
        return Template(components)

    def __repr__(self) -> str:
        """Return a string representation."""
        return f"Template({self._components!r})"

    def __eq__(self, other: object) -> bool:
        """Check equality."""
        if not isinstance(other, Template):
            return NotImplemented
        return self._components == other._components

    def __add__(self, other: Union[Template, str]) -> Template:
        """Concatenate templates."""
        if isinstance(other, str):
            return Template([*self._components, other])
        elif isinstance(other, Template):
            return Template([*self._components, *other._components])
        return NotImplemented

    def __radd__(self, other: str) -> Template:
        """Concatenate with string on left."""
        if isinstance(other, str):
            return Template([other, *self._components])
        return NotImplemented


def template(*components: Component) -> Template:
    """Create a template from components."""
    # Merge adjacent strings.
    merged: List[Component] = []
    for component in components:
        if isinstance(component, str) and merged and isinstance(merged[-1], str):
            merged[-1] = merged[-1] + component
        else:
            merged.append(component)
    return Template(merged)
