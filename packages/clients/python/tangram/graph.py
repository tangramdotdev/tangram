"""Graph class and utilities.

This module mirrors the JavaScript packages/clients/js/src/graph.ts.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, TypedDict, Union
from urllib.parse import quote, unquote

from tangram.object_ import Object_
from tangram.reference import Reference
from tangram.referent import Options as ReferentOptions
from tangram.referent import Referent


async def graph(*args: Any) -> "Graph":
    """Create a new graph."""
    return await Graph.new(*args)


class Graph:
    """A graph is a collection of nodes representing artifacts."""

    _state: Object_.State

    def __init__(
        self,
        *,
        id: Optional[str] = None,
        object: Optional[Dict[str, Any]] = None,
        stored: bool = False,
    ) -> None:
        """Initialize a graph."""
        obj = {"kind": "graph", "value": object} if object is not None else None
        self._state = Object_.State(id=id, object=obj, stored=stored)

    @property
    def state(self) -> Object_.State:
        """Get the object state."""
        return self._state

    @classmethod
    def with_id(cls, id: str) -> "Graph":
        """Create a graph with the given ID."""
        return cls(id=id, stored=True)

    @classmethod
    def with_object(cls, object: Dict[str, Any]) -> "Graph":
        """Create a graph with the given object."""
        return cls(object=object, stored=False)

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "Graph":
        """Create a graph from serialized data."""
        return cls.with_object(GraphObject.from_data(data))

    @classmethod
    async def new(cls, *args: Any) -> "Graph":
        """Create a new graph from arguments."""
        import tangram as tg

        arg = await cls.arg(*args)
        nodes = []
        for node in arg.get("nodes") or []:
            if node["kind"] == "directory":
                entries = {}
                for name, edge in (node.get("entries") or {}).items():
                    entries[name] = Edge.from_arg(edge, arg.get("nodes"))
                nodes.append({"kind": "directory", "entries": entries})
            elif node["kind"] == "file":
                contents = await tg.blob(node.get("contents"))
                dependencies = {}
                for reference, value in (node.get("dependencies") or {}).items():
                    if not value:
                        dependencies[reference] = None
                        continue
                    if (
                        isinstance(value, int)
                        or (isinstance(value, dict) and "index" in value)
                        or Object_.is_(value)
                    ):
                        item = Edge.from_arg(value, arg.get("nodes"))
                        dependencies[reference] = {"item": item, "options": {}}
                    elif value.get("item") is None:
                        dependencies[reference] = {
                            "item": None,
                            "options": value.get("options", {}),
                        }
                    else:
                        item = Edge.from_arg(value["item"], arg.get("nodes"))
                        dependencies[reference] = {
                            "item": item,
                            "options": value.get("options", {}),
                        }
                executable = node.get("executable", False)
                nodes.append(
                    {
                        "kind": "file",
                        "contents": contents,
                        "dependencies": dependencies,
                        "executable": executable,
                    }
                )
            elif node["kind"] == "symlink":
                artifact = Edge.from_arg(node.get("artifact"), arg.get("nodes"))
                path = node.get("path")
                nodes.append({"kind": "symlink", "artifact": artifact, "path": path})
        return cls.with_object({"nodes": nodes})

    @classmethod
    async def arg(cls, *args: Any) -> Dict[str, Any]:
        """Process graph arguments."""
        from tangram.resolve import resolve

        resolved = [await resolve(arg) for arg in args]
        nodes: List[Dict[str, Any]] = []
        offset = 0
        for arg in resolved:
            if isinstance(arg, Graph):
                arg_nodes = await arg.nodes()
            elif isinstance(arg, dict) and "nodes" in arg:
                arg_nodes = arg.get("nodes") or []
            else:
                arg_nodes = []

            for arg_node in arg_nodes:
                if arg_node["kind"] == "directory":
                    node: Dict[str, Any] = {"kind": "directory"}
                    if "entries" in arg_node and arg_node["entries"] is not None:
                        node["entries"] = {}
                        for name, entry in arg_node["entries"].items():
                            if isinstance(entry, int):
                                node["entries"][name] = entry + offset
                            elif (
                                isinstance(entry, dict)
                                and "index" in entry
                                and entry.get("graph") is None
                            ):
                                node["entries"][name] = {
                                    **entry,
                                    "index": entry["index"] + offset,
                                }
                            elif entry:
                                node["entries"][name] = entry
                    nodes.append(node)
                elif arg_node["kind"] == "file":
                    node = {"kind": "file"}
                    if "contents" in arg_node:
                        node["contents"] = arg_node["contents"]
                    if "dependencies" in arg_node:
                        if arg_node["dependencies"] is not None:
                            node["dependencies"] = {}
                            for ref, value in arg_node["dependencies"].items():
                                if value is None:
                                    node["dependencies"][ref] = None
                                elif (
                                    isinstance(value, int)
                                    or (isinstance(value, dict) and "index" in value)
                                    or Object_.is_(value)
                                ):
                                    node["dependencies"][ref] = {
                                        "item": value,
                                        "options": {},
                                    }
                                else:
                                    dep = value
                                    if dep.get("item") is None:
                                        node["dependencies"][ref] = dep
                                    elif isinstance(dep["item"], int):
                                        node["dependencies"][ref] = {
                                            "item": dep["item"] + offset,
                                            "options": dep.get("options", {}),
                                        }
                                    elif (
                                        isinstance(dep["item"], dict)
                                        and "index" in dep["item"]
                                    ):
                                        node["dependencies"][ref] = {
                                            "item": {
                                                **dep["item"],
                                                "index": dep["item"]["index"] + offset,
                                            },
                                            "options": dep.get("options", {}),
                                        }
                                    elif Object_.is_(dep["item"]):
                                        node["dependencies"][ref] = dep
                    if "executable" in arg_node:
                        node["executable"] = arg_node["executable"]
                    nodes.append(node)
                elif arg_node["kind"] == "symlink":
                    artifact = arg_node.get("artifact")
                    if isinstance(artifact, int):
                        artifact = artifact + offset
                    elif (
                        artifact is not None
                        and isinstance(artifact, dict)
                        and "index" in artifact
                    ):
                        artifact = {**artifact, "index": artifact["index"] + offset}
                    nodes.append(
                        {
                            "kind": "symlink",
                            "artifact": artifact,
                            "path": arg_node.get("path"),
                        }
                    )
            offset += len(arg_nodes)
        return {"nodes": nodes}

    @staticmethod
    def expect(value: Any) -> "Graph":
        """Assert that value is a Graph and return it."""
        from tangram.assert_ import assert_

        assert_(isinstance(value, Graph))
        return value

    @staticmethod
    def assert_(value: Any) -> None:
        """Assert that value is a Graph."""
        from tangram.assert_ import assert_

        assert_(isinstance(value, Graph))

    @property
    def id(self) -> str:
        """Get the graph ID."""
        from tangram.assert_ import assert_

        id = self._state.id
        assert_(Object_.Id.kind(id) == "graph")
        return id

    async def object(self) -> Dict[str, Any]:
        """Get the graph object."""
        from tangram.assert_ import assert_

        obj = await self._state.load()
        assert_(obj.get("kind") == "graph")
        return obj.get("value", obj)

    async def load(self) -> Dict[str, Any]:
        """Load the graph object."""
        from tangram.assert_ import assert_

        obj = await self._state.load()
        assert_(obj.get("kind") == "graph")
        return obj.get("value", obj)

    def unload(self) -> None:
        """Unload the graph data."""
        self._state.unload()

    async def store(self) -> str:
        """Store the graph and return its ID."""
        import tangram as tg

        await tg.Value.store(self)
        return self.id

    async def children(self) -> List[Object_]:
        """Get child objects."""
        return await self._state.children()

    async def nodes(self) -> List[Dict[str, Any]]:
        """Get the graph nodes."""
        obj = await self.object()
        return obj.get("nodes", [])

    async def get(self, index: int) -> Any:
        """Get an artifact at the given index."""
        from tangram.assert_ import assert_
        from tangram.directory import Directory
        from tangram.file import File
        from tangram.symlink import Symlink

        nodes = await self.nodes()
        node = nodes[index] if index < len(nodes) else None
        assert_(node is not None, "invalid graph index")

        if node["kind"] == "directory":
            return Directory.with_object(
                {"graph": self, "index": index, "kind": "directory"}
            )
        elif node["kind"] == "file":
            return File.with_object({"graph": self, "index": index, "kind": "file"})
        elif node["kind"] == "symlink":
            return Symlink.with_object(
                {"graph": self, "index": index, "kind": "symlink"}
            )


# Nested namespace classes.
class GraphObject:
    """Namespace for Graph.Object operations."""

    @staticmethod
    def to_data(obj: Dict[str, Any]) -> Dict[str, Any]:
        """Convert object to data."""
        return {"nodes": [Node.to_data(node) for node in obj.get("nodes", [])]}

    @staticmethod
    def from_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert data to object."""
        return {"nodes": [Node.from_data(node) for node in data.get("nodes", [])]}

    @staticmethod
    def children(obj: Dict[str, Any]) -> List[Object_]:
        """Get child objects from graph object."""
        result: List[Object_] = []
        for node in obj.get("nodes", []):
            result.extend(Node.children(node))
        return result


class Node:
    """Namespace for Graph.Node operations."""

    @staticmethod
    def to_data(node: Dict[str, Any]) -> Dict[str, Any]:
        """Convert node to data."""
        if node["kind"] == "directory":
            return {"kind": "directory", **DirectoryNode.to_data(node)}
        elif node["kind"] == "file":
            return {"kind": "file", **FileNode.to_data(node)}
        elif node["kind"] == "symlink":
            return {"kind": "symlink", **SymlinkNode.to_data(node)}
        else:
            return node

    @staticmethod
    def from_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert data to node."""
        if data["kind"] == "directory":
            return {"kind": "directory", **DirectoryNode.from_data(data)}
        elif data["kind"] == "file":
            return {"kind": "file", **FileNode.from_data(data)}
        elif data["kind"] == "symlink":
            return {"kind": "symlink", **SymlinkNode.from_data(data)}
        else:
            return data

    @staticmethod
    def children(node: Dict[str, Any]) -> List[Object_]:
        """Get child objects from node."""
        if node["kind"] == "directory":
            return DirectoryNode.children(node)
        elif node["kind"] == "file":
            return FileNode.children(node)
        elif node["kind"] == "symlink":
            return SymlinkNode.children(node)
        return []


class DirectoryNode:
    """Namespace for Graph.Directory operations."""

    @staticmethod
    def to_data(node: Dict[str, Any]) -> Dict[str, Any]:
        """Convert directory node to data."""
        from tangram.artifact import Artifact

        data: Dict[str, Any] = {}
        entries = node.get("entries", {})
        if entries:
            data["entries"] = {
                name: Edge.to_data(entry, lambda a: a.id)
                for name, entry in entries.items()
            }
        return data

    @staticmethod
    def from_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert data to directory node."""
        from tangram.artifact import with_id as artifact_with_id

        return {
            "entries": {
                name: Edge.from_data(edge, artifact_with_id)
                for name, edge in (data.get("entries") or {}).items()
            }
        }

    @staticmethod
    def children(node: Dict[str, Any]) -> List[Object_]:
        """Get child objects from directory node."""
        result: List[Object_] = []
        for edge in (node.get("entries") or {}).values():
            result.extend(Edge.children(edge))
        return result


class FileNode:
    """Namespace for Graph.File operations."""

    @staticmethod
    def to_data(node: Dict[str, Any]) -> Dict[str, Any]:
        """Convert file node to data."""
        data: Dict[str, Any] = {}
        contents = node.get("contents")
        if contents is not None:
            data["contents"] = contents.id if hasattr(contents, "id") else contents
        dependencies = node.get("dependencies", {})
        if dependencies:
            data["dependencies"] = {}
            for reference, dependency in dependencies.items():
                if dependency is None:
                    data["dependencies"][reference] = None
                else:
                    data["dependencies"][reference] = Dependency.to_data_string(
                        dependency
                    )
        executable = node.get("executable", False)
        if executable:
            data["executable"] = executable
        return data

    @staticmethod
    def from_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert data to file node."""
        from tangram.assert_ import assert_
        from tangram.blob import Blob

        contents_data = data.get("contents")
        assert_(contents_data is not None)
        return {
            "contents": Blob.with_id(contents_data),
            "dependencies": {
                reference: (
                    Dependency.from_data_string(dep)
                    if isinstance(dep, str)
                    else Dependency.from_data(dep)
                    if dep is not None
                    else None
                )
                for reference, dep in (data.get("dependencies") or {}).items()
            },
            "executable": data.get("executable", False),
        }

    @staticmethod
    def children(node: Dict[str, Any]) -> List[Object_]:
        """Get child objects from file node."""
        result: List[Object_] = []
        contents = node.get("contents")
        if contents is not None:
            result.append(contents)
        for dependency in (node.get("dependencies") or {}).values():
            if dependency is not None and dependency.get("item") is not None:
                result.extend(Edge.children(dependency["item"]))
        return result


class SymlinkNode:
    """Namespace for Graph.Symlink operations."""

    @staticmethod
    def to_data(node: Dict[str, Any]) -> Dict[str, Any]:
        """Convert symlink node to data."""
        data: Dict[str, Any] = {}
        artifact = node.get("artifact")
        if artifact is not None:
            data["artifact"] = Edge.to_data(artifact, lambda a: a.id)
        path = node.get("path")
        if path is not None:
            data["path"] = path
        return data

    @staticmethod
    def from_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert data to symlink node."""
        from tangram.artifact import with_id as artifact_with_id

        return {
            "artifact": (
                Edge.from_data(data["artifact"], artifact_with_id)
                if data.get("artifact") is not None
                else None
            ),
            "path": data.get("path"),
        }

    @staticmethod
    def children(node: Dict[str, Any]) -> List[Object_]:
        """Get child objects from symlink node."""
        artifact = node.get("artifact")
        if artifact is not None:
            return Edge.children(artifact)
        return []


class Edge:
    """Namespace for Graph.Edge operations."""

    @staticmethod
    def from_arg(
        arg: Any, nodes: Optional[List[Dict[str, Any]]] = None
    ) -> Any:
        """Convert an argument to an edge."""
        if isinstance(arg, int):
            if nodes is None:
                raise ValueError("cannot convert number to edge without nodes context")
            kind = nodes[arg].get("kind") if arg < len(nodes) else None
            if not kind:
                raise ValueError(f"invalid node index: {arg}")
            return {"index": arg, "kind": kind}
        elif isinstance(arg, dict) and "index" in arg:
            if arg.get("kind") is not None:
                return arg
            if nodes is None:
                raise ValueError("cannot infer kind without nodes context")
            kind = (
                nodes[arg["index"]].get("kind") if arg["index"] < len(nodes) else None
            )
            if not kind:
                raise ValueError(f"invalid node index: {arg['index']}")
            return {**arg, "kind": kind}
        else:
            return arg

    @staticmethod
    def to_data(obj: Any, f: Any) -> Any:
        """Convert edge to data."""
        if isinstance(obj, dict) and "index" in obj:
            return Pointer.to_data(obj)
        else:
            return f(obj)

    @staticmethod
    def from_data(data: Any, f: Any) -> Any:
        """Convert data to edge."""
        if isinstance(data, str) or (isinstance(data, dict) and "index" in data):
            try:
                return Pointer.from_data(data)
            except Exception:
                pass
        return f(data)

    @staticmethod
    def to_data_string(obj: Any, f: Any) -> str:
        """Convert edge to string."""
        if isinstance(obj, dict) and "index" in obj:
            return Pointer.to_data_string(obj)
        else:
            return f(obj)

    @staticmethod
    def from_data_string(data: str, f: Any) -> Any:
        """Convert string to edge."""
        if "index=" in data:
            return Pointer.from_data_string(data)
        else:
            return f(data)

    @staticmethod
    def children(obj: Any) -> List[Object_]:
        """Get child objects from edge."""
        if isinstance(obj, int):
            return []
        elif isinstance(obj, dict) and "index" in obj:
            return Pointer.children(obj)
        elif Object_.is_(obj):
            return [obj]
        return []


class Pointer:
    """Namespace for Graph.Pointer operations."""

    @staticmethod
    def to_data(obj: Dict[str, Any]) -> Dict[str, Any]:
        """Convert pointer to data."""
        data: Dict[str, Any] = {"index": obj["index"], "kind": obj["kind"]}
        if obj.get("graph") is not None:
            data["graph"] = obj["graph"].id
        return data

    @staticmethod
    def from_data(data: Any) -> Dict[str, Any]:
        """Convert data to pointer."""
        if isinstance(data, str):
            return Pointer.from_data_string(data)
        graph = Graph.with_id(data["graph"]) if data.get("graph") else None
        return {"graph": graph, "index": data["index"], "kind": data["kind"]}

    @staticmethod
    def to_data_string(obj: Dict[str, Any]) -> str:
        """Convert pointer to string."""
        string = ""
        if obj.get("graph") is not None:
            string += f"graph={obj['graph'].id}&"
        string += f"index={obj['index']}"
        string += f"&kind={obj['kind']}"
        return string

    @staticmethod
    def from_data_string(data: str) -> Dict[str, Any]:
        """Convert string to pointer."""
        from tangram.assert_ import assert_

        graph: Optional[Graph] = None
        index: Optional[int] = None
        kind: Optional[str] = None

        for param in data.split("&"):
            parts = param.split("=", 1)
            if len(parts) != 2:
                raise ValueError("missing value")
            key, value = parts
            decoded = unquote(value)
            if key == "graph":
                graph = Graph.with_id(decoded)
            elif key == "index":
                index = int(decoded)
            elif key == "kind":
                kind = decoded

        assert_(index is not None, "missing index")
        assert_(kind is not None, "missing kind")
        return {"graph": graph, "index": index, "kind": kind}

    @staticmethod
    def children(obj: Dict[str, Any]) -> List[Object_]:
        """Get child objects from pointer."""
        if obj.get("graph") is not None:
            return [obj["graph"]]
        return []

    @staticmethod
    def is_(value: Any) -> bool:
        """Check if value is a pointer."""
        return (
            isinstance(value, dict)
            and "index" in value
            and isinstance(value["index"], int)
        )


class Dependency:
    """Namespace for Graph.Dependency operations."""

    @staticmethod
    def to_data_string(value: Dict[str, Any]) -> str:
        """Convert dependency to string."""
        item = value.get("item")
        if item is not None:
            item_str = Edge.to_data_string(item, lambda i: i.id)
        else:
            item_str = ""
        params = []
        options = value.get("options", {})
        if options.get("artifact") is not None:
            params.append(f"artifact={quote(options['artifact'])}")
        if options.get("id") is not None:
            params.append(f"id={quote(options['id'])}")
        if options.get("name") is not None:
            params.append(f"name={quote(options['name'])}")
        if options.get("path") is not None:
            params.append(f"path={quote(options['path'])}")
        if options.get("tag") is not None:
            params.append(f"tag={quote(options['tag'])}")
        if params:
            item_str += "?"
            item_str += "&".join(params)
        return item_str

    @staticmethod
    def from_data_string(data: str) -> Dict[str, Any]:
        """Convert string to dependency."""
        parts = data.split("?", 1)
        item_string = parts[0]
        params = parts[1] if len(parts) > 1 else None

        item: Any = None
        if item_string:
            item = Edge.from_data_string(item_string, Object_.with_id)

        options: Dict[str, Any] = {}
        if params:
            for param in params.split("&"):
                key_value = param.split("=", 1)
                if len(key_value) != 2:
                    raise ValueError("missing value")
                key, value = key_value
                decoded = unquote(value)
                if key == "artifact":
                    options["artifact"] = decoded
                elif key == "id":
                    options["id"] = decoded
                elif key == "name":
                    options["name"] = decoded
                elif key == "path":
                    options["path"] = decoded
                elif key == "tag":
                    options["tag"] = decoded

        return {"item": item, "options": options}

    @staticmethod
    def from_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert data to dependency."""
        item = data.get("item")
        if item is not None:
            item = Edge.from_data(item, Object_.with_id)
        return {"item": item, "options": data.get("options", {})}


# Set up nested class references.
Graph.Object = GraphObject
Graph.Node = Node
Graph.Directory = DirectoryNode
Graph.File = FileNode
Graph.Symlink = SymlinkNode
Graph.Edge = Edge
Graph.Pointer = Pointer
Graph.Dependency = Dependency

# Type aliases.
Graph.Id = str
