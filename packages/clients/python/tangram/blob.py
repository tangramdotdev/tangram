"""Blob class for Tangram.

This module mirrors the JavaScript packages/clients/js/src/blob.ts.
"""

from __future__ import annotations

import base64
from typing import Any, Dict, List, Optional, Union

from tangram.object_ import Object_


async def blob(*args: Any) -> "Blob":
    """Create a new blob."""
    return await Blob.new(*args)


class Blob:
    """A blob is a binary large object."""

    _state: Object_.State

    def __init__(
        self,
        *,
        id: Optional[str] = None,
        object: Optional[Dict[str, Any]] = None,
        stored: bool = False,
    ) -> None:
        """Initialize a blob."""
        obj = {"kind": "blob", "value": object} if object is not None else None
        self._state = Object_.State(id=id, object=obj, stored=stored)

    @property
    def state(self) -> Object_.State:
        """Get the object state."""
        return self._state

    @classmethod
    def with_id(cls, id: str) -> "Blob":
        """Create a blob with the given ID."""
        return cls(id=id, stored=True)

    @classmethod
    def with_object(cls, object: Dict[str, Any]) -> "Blob":
        """Create a blob with the given object."""
        return cls(object=object, stored=False)

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "Blob":
        """Create a blob from serialized data."""
        return cls.with_object(Blob.Object.from_data(data))

    @classmethod
    async def new(cls, *args: Any) -> "Blob":
        """Create a new blob from arguments."""
        import tangram as tg

        arg = await cls.arg(*args)
        children = arg.get("children", [])
        if not children:
            return cls.with_object({"bytes": b""})
        elif len(children) == 1:
            return children[0]["blob"]
        else:
            return cls.with_object({"children": children})

    @classmethod
    async def leaf(cls, *args: Any) -> "Blob":
        """Create a leaf blob from raw data."""
        import tangram as tg

        resolved = await tg.resolve(list(args))
        objects = []
        for arg in resolved:
            if arg is None:
                objects.append(b"")
            elif isinstance(arg, str):
                objects.append(arg.encode("utf-8"))
            elif isinstance(arg, bytes):
                objects.append(arg)
            elif isinstance(arg, Blob):
                objects.append(await arg.bytes())
            else:
                objects.append(b"")
        data = b"".join(objects)
        return cls.with_object({"bytes": data})

    @classmethod
    async def branch(cls, *args: Any) -> "Blob":
        """Create a branch blob."""
        arg = await cls.arg(*args)
        return cls.with_object({"children": arg.get("children", [])})

    @classmethod
    async def arg(cls, *args: Any) -> Dict[str, Any]:
        """Process blob arguments using Args.apply."""
        import tangram as tg

        async def map_fn(arg: Any) -> Dict[str, Any]:
            if arg is None:
                return {"children": []}
            elif isinstance(arg, str):
                data = arg.encode("utf-8")
                blob = Blob.with_object({"bytes": data})
                return {"children": [{"blob": blob, "length": len(data)}]}
            elif isinstance(arg, bytes):
                blob = Blob.with_object({"bytes": arg})
                return {"children": [{"blob": blob, "length": len(arg)}]}
            elif isinstance(arg, Blob):
                length = await arg.length()
                return {"children": [{"blob": arg, "length": length}]}
            elif isinstance(arg, dict):
                return arg
            else:
                return {"children": []}

        return await tg.Args.apply(
            args=list(args),
            map_fn=map_fn,
            reduce_spec={"children": "append"},
        )

    @staticmethod
    def expect(value: Any) -> "Blob":
        """Assert that value is a Blob and return it."""
        from tangram.assert_ import assert_

        assert_(isinstance(value, Blob))
        return value

    @staticmethod
    def assert_(value: Any) -> None:
        """Assert that value is a Blob."""
        from tangram.assert_ import assert_

        assert_(isinstance(value, Blob))

    @property
    def id(self) -> str:
        """Get the blob ID."""
        from tangram.assert_ import assert_

        id = self._state.id
        assert_(Object_.Id.kind(id) == "blob")
        return id

    async def object(self) -> Dict[str, Any]:
        """Get the blob object."""
        from tangram.assert_ import assert_

        obj = await self._state.load()
        assert_(obj.get("kind") == "blob")
        return obj.get("value", obj)

    async def load(self) -> Dict[str, Any]:
        """Load the blob object."""
        from tangram.assert_ import assert_

        obj = await self._state.load()
        assert_(obj.get("kind") == "blob")
        return obj.get("value", obj)

    def unload(self) -> None:
        """Unload the blob data."""
        self._state.unload()

    async def store(self) -> str:
        """Store the blob and return its ID."""
        import tangram as tg

        await tg.Value.store(self)
        return self.id

    async def children(self) -> List[Object_]:
        """Get child objects."""
        return await self._state.children()

    async def length(self) -> int:
        """Get the blob length."""
        obj = await self.object()
        if "children" in obj:
            return sum(child["length"] for child in obj["children"])
        else:
            return len(obj.get("bytes", b""))

    async def read(
        self,
        position: Optional[int] = None,
        length: Optional[int] = None,
        size: Optional[int] = None,
    ) -> bytes:
        """Read blob contents."""
        import tangram as tg

        id = await self.store()
        arg: Dict[str, Any] = {"blob": id}
        if position is not None:
            arg["position"] = position
        if length is not None:
            arg["length"] = length
        if size is not None:
            arg["size"] = size
        return await tg.handle.read(arg)

    async def bytes(self) -> bytes:
        """Get the blob contents as bytes."""
        return await self.read()

    async def text(self) -> str:
        """Get the blob contents as text."""
        data = await self.bytes()
        return data.decode("utf-8")

    # Nested Object namespace.
    class Object:
        """Namespace for Blob.Object operations."""

        @staticmethod
        def to_data(obj: Dict[str, Any]) -> Dict[str, Any]:
            """Convert object to data."""
            if "bytes" in obj:
                return {"bytes": base64.b64encode(obj["bytes"]).decode("ascii")}
            else:
                return {
                    "children": [
                        {"blob": child["blob"].id, "length": child["length"]}
                        for child in obj.get("children", [])
                    ]
                }

        @staticmethod
        def from_data(data: Dict[str, Any]) -> Dict[str, Any]:
            """Convert data to object."""
            if "bytes" in data:
                return {"bytes": base64.b64decode(data["bytes"])}
            else:
                return {
                    "children": [
                        {"blob": Blob.with_id(child["blob"]), "length": child["length"]}
                        for child in data.get("children", [])
                    ]
                }

        @staticmethod
        def children(obj: Optional[Dict[str, Any]]) -> List[Object_]:
            """Get child objects from blob object."""
            if obj is None:
                return []
            if "children" in obj:
                return [child["blob"] for child in obj["children"]]
            return []


# Type aliases.
Blob.Id = str
Blob.Arg = Union[None, str, bytes, "Blob", Dict[str, Any]]
Blob.Data = Dict[str, Any]
