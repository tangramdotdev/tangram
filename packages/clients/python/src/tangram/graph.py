from __future__ import annotations

import builtins
import json
import math
import re
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    NotRequired,
    Required,
    Self,
    TypedDict,
    TypeGuard,
    cast,
    overload,
)

import tangram as tg

from . import _native
from .builder import Builder
from .mutation import UNSET
from .object import Object
from .referent import DataOptions, Referent, decode_uri_component
from .resolve import Unresolved, resolve

if TYPE_CHECKING:
    from .blob import Blob, BlobInput
    from .client import Client
    from .directory import Directory
    from .file import File
    from .symlink import Symlink

# The maximum number of children of a graph branch.
MAX_BRANCH_CHILDREN = 1024

type ArtifactValue = Directory | File | Symlink


class GraphDirectoryChild(TypedDict):
    directory: Directory | Pointer | int
    count: int
    last: str


class DirectoryLeafNode(TypedDict):
    kind: Literal["directory"]
    entries: dict[str, ArtifactValue | Pointer | int]


class DirectoryBranchNode(TypedDict):
    kind: Literal["directory"]
    children: list[GraphDirectoryChild]


type DirectoryNode = DirectoryLeafNode | DirectoryBranchNode


class FileNode(TypedDict, total=False):
    kind: Required[Literal["file"]]
    contents: Blob
    dependencies: dict[str, Referent | None]
    executable: bool
    module: str | None


class SymlinkNode(TypedDict, total=False):
    kind: Required[Literal["symlink"]]
    artifact: ArtifactValue | Pointer | int | None
    path: str | None


type GraphNode = DirectoryNode | FileNode | SymlinkNode


class PointerArg(TypedDict):
    graph: Graph
    index: int
    kind: Literal["directory", "file", "symlink"]


type GraphEdge[T] = T | Pointer | int
type GraphArgEdge[T] = T | Pointer | PointerArg | int
type GraphEdgeInput = Unresolved[GraphArgEdge[Object]]


class DirectoryNodeArg(TypedDict, total=False):
    kind: Required[Literal["directory"]]
    entries: Unresolved[dict[str, GraphEdgeInput]]


class FileNodeArg(TypedDict, total=False):
    kind: Required[Literal["file"]]
    contents: BlobInput
    dependencies: Unresolved[
        dict[str, Unresolved[Referent | GraphEdgeInput | None]] | None
    ]
    executable: Unresolved[bool | None]
    module: Unresolved[str | None]


class SymlinkNodeArg(TypedDict, total=False):
    kind: Required[Literal["symlink"]]
    artifact: GraphEdgeInput | None
    path: Unresolved[str | None]


type GraphNodeInput = Unresolved[DirectoryNodeArg | FileNodeArg | SymlinkNodeArg]


class GraphArgObject(TypedDict, total=False):
    nodes: Unresolved[list[GraphNodeInput] | None]


class GraphLeafValue(TypedDict):
    nodes: list[GraphNode]


class GraphChild(TypedDict):
    count: int
    graph: Graph


class GraphBranchValue(TypedDict):
    children: list[GraphChild]


type GraphValue = GraphLeafValue | GraphBranchValue


class GraphConstructorArg(TypedDict, total=False):
    id: str
    object: GraphValue
    stored: Required[bool]
    tokens: dict[str, list[str]] | None


type GraphInput = Unresolved[
    Graph | GraphArgObject | list[GraphInput] | tuple[GraphInput, ...]
]


class GraphDataPointerObject(TypedDict):
    graph: str
    index: int
    kind: Literal["directory", "file", "symlink"]


type GraphDataPointer = str | GraphDataPointerObject
type GraphDataEdge[T] = int | GraphDataPointer | T


class GraphDependencyDataObject(TypedDict):
    node: GraphDataEdge[str] | None
    options: NotRequired[DataOptions]


type GraphDependencyData = str | GraphDependencyDataObject


class GraphDataDirectoryLeaf(TypedDict):
    entries: dict[str, GraphDataEdge[str]]


class GraphDataDirectoryChild(TypedDict):
    directory: GraphDataEdge[str]
    count: int
    last: str


class GraphDataDirectoryBranch(TypedDict):
    children: list[GraphDataDirectoryChild]


class GraphDataFile(TypedDict, total=False):
    contents: str | None
    dependencies: dict[str, GraphDependencyData | None]
    executable: bool
    module: str | None


class GraphDataSymlink(TypedDict, total=False):
    artifact: GraphDataEdge[str] | None
    path: str | None


class GraphDataDirectoryLeafNode(GraphDataDirectoryLeaf):
    kind: Literal["directory"]


class GraphDataDirectoryBranchNode(GraphDataDirectoryBranch):
    kind: Literal["directory"]


class GraphDataFileNode(GraphDataFile):
    kind: Literal["file"]


class GraphDataSymlinkNode(GraphDataSymlink):
    kind: Literal["symlink"]


type GraphDataNode = (
    GraphDataDirectoryLeafNode
    | GraphDataDirectoryBranchNode
    | GraphDataFileNode
    | GraphDataSymlinkNode
)


class GraphDataLeaf(TypedDict):
    nodes: list[GraphDataNode]


class GraphDataChild(TypedDict):
    count: int
    graph: str


class GraphDataBranch(TypedDict):
    children: list[GraphDataChild]


type GraphData = GraphDataLeaf | GraphDataBranch


class Graph(Object):
    kind = "graph"
    Builder: ClassVar[type[GraphBuilder]]
    Object: ClassVar[type[GraphObject]]
    Data: ClassVar[type[GraphDataNamespace]]
    Arg: ClassVar[type[Arg]]
    Node: ClassVar[type[Node]]
    Directory: ClassVar[type[GraphDirectory]]
    File: ClassVar[type[GraphFile]]
    Dependency: ClassVar[type[Dependency]]
    Symlink: ClassVar[type[GraphSymlink]]
    Edge: ClassVar[type[Edge]]
    Pointer: ClassVar[type[Pointer]]
    ConstructorArg: ClassVar[type[GraphConstructorArg]]
    DirectoryNode: Any
    FileNode: Any
    SymlinkNode: Any
    DirectoryLeaf: Any
    DirectoryBranch: Any
    DirectoryChild: Any

    @overload
    def __init__(self, nodes: GraphConstructorArg, **options) -> None: ...

    @overload
    def __init__(self, nodes: list[GraphNode] | None = None, **options) -> None: ...

    def __init__(self, nodes=None, **options) -> None:
        if isinstance(nodes, dict) and any(
            key in nodes for key in ("object", "id", "stored")
        ):
            arg = nodes
            super().__init__(
                arg.get("object"),
                id=arg.get("id"),
                tokens=arg.get("tokens"),
            )
            self._stored = arg.get("stored", False)
        else:
            if nodes is not None:
                nodes = [dict(node) for node in nodes]
                for node in nodes:
                    if node["kind"] == "file":
                        node.setdefault("dependencies", {})
                        node.setdefault("executable", False)
                        node.setdefault("module", None)
                    elif node["kind"] == "symlink":
                        node.setdefault("artifact", None)
                        node.setdefault("path", None)
            super().__init__({"nodes": nodes} if nodes is not None else None, **options)

    async def object(self, client: Client | None = None) -> GraphValue:
        return await self.load(client)

    async def load(self, client: Client | None = None) -> GraphValue:
        return cast(GraphValue, await super().load(client))

    @classmethod
    async def new(cls, *args: GraphInput, client: Client | None = None) -> Self:
        from .blob import Blob

        resolved_args = await resolve(args)
        if len(resolved_args) == 1 and isinstance(resolved_args[0], cls):
            return resolved_args[0]
        arg = await cls.arg_resolved(*resolved_args, client=client)
        nodes = []
        for node in arg["nodes"]:
            kind = node["kind"]
            if kind == "directory":
                nodes.append(
                    {
                        "kind": kind,
                        "entries": {
                            name: Edge.from_arg(edge, arg["nodes"])
                            for name, edge in (
                                optional(node.get("entries")) or {}
                            ).items()
                        },
                    }
                )
            elif kind == "file":
                dependencies = {}
                for reference, value in (
                    optional(node.get("dependencies")) or {}
                ).items():
                    if value is UNSET:
                        continue
                    if value is None:
                        dependencies[reference] = None
                    else:
                        dependency = as_dependency(value)
                        dependencies[reference] = Referent(
                            None
                            if dependency.node is None
                            else Edge.from_arg(dependency.node, arg["nodes"]),
                            dependency.options,
                        )
                nodes.append(
                    {
                        "kind": kind,
                        "contents": await Blob.new(node.get("contents"), client=client),
                        "dependencies": dependencies,
                        "executable": optional(node.get("executable")) or False,
                        "module": optional(node.get("module")),
                    }
                )
            elif kind == "symlink":
                nodes.append(
                    {
                        "kind": kind,
                        "artifact": None
                        if optional(node.get("artifact")) is None
                        else Edge.from_arg(node["artifact"], arg["nodes"]),
                        "path": optional(node.get("path")),
                    }
                )
            else:
                raise AssertionError("unreachable")
        return cls.with_nodes(cast(list[GraphNode], nodes))

    @classmethod
    def with_nodes(cls, nodes: list[GraphNode]) -> Self:
        """Create a graph with nodes, splitting them into leaves like checkin."""
        # Get the number of nodes in each leaf.
        data = {"nodes": [Node.to_data(node) for node in nodes]}
        counts = _native.graph_leaf_counts(json.dumps(data, allow_nan=False))

        # Create a single leaf if the nodes fit in one.
        if len(counts) == 1:
            return cls.with_object({"nodes": nodes})

        # Create the leaves.
        children: list[GraphChild] = []
        offset = 0
        for count in counts:
            leaf = Graph.with_object({"nodes": nodes[offset : offset + count]})
            children.append({"count": count, "graph": leaf})
            offset += count

        # Nest the children in branches until they fit in one branch.
        while len(children) > MAX_BRANCH_CHILDREN:
            branches: list[GraphChild] = []
            for start in range(0, len(children), MAX_BRANCH_CHILDREN):
                chunk = children[start : start + MAX_BRANCH_CHILDREN]
                count = sum(child["count"] for child in chunk)
                branch = Graph.with_object({"children": chunk})
                branches.append({"count": count, "graph": branch})
            children = branches

        # Create the root branch.
        return cls.with_object({"children": children})

    @classmethod
    async def arg(cls, *args, client: Client | None = None):
        return await cls.arg_resolved(*(await resolve(args)), client=client)

    @classmethod
    async def arg_resolved(cls, *args, client: Client | None = None):
        nodes = []
        offset = 0
        for arg in args:
            if arg is UNSET:
                continue
            if not isinstance(arg, cls) and arg.get("nodes", UNSET) is None:
                nodes = []
                offset = 0
                continue
            arg_nodes = (
                await arg.nodes(client)
                if isinstance(arg, cls)
                else arg.get("nodes") or []
            )
            for arg_node in arg_nodes:
                node = {"kind": arg_node["kind"]}
                if node["kind"] == "directory":
                    if "entries" in arg_node:
                        entries = arg_node["entries"]
                        node["entries"] = (
                            UNSET
                            if entries is UNSET
                            else {
                                name: edge + offset if is_number(edge) else edge
                                for name, edge in entries.items()
                                if is_number(edge) or edge
                            }
                        )
                elif node["kind"] == "file":
                    for key in ("contents", "executable", "module"):
                        if key in arg_node:
                            node[key] = arg_node[key]
                    if "dependencies" in arg_node:
                        dependencies = arg_node["dependencies"]
                        if dependencies is UNSET:
                            node["dependencies"] = UNSET
                        else:
                            node["dependencies"] = {}
                            for reference, value in (dependencies or {}).items():
                                if value is UNSET:
                                    continue
                                if value is None:
                                    node["dependencies"][reference] = None
                                else:
                                    dependency = as_dependency(value)
                                    node["dependencies"][reference] = Referent(
                                        dependency.node + offset
                                        if is_number(dependency.node)
                                        else dependency.node,
                                        dependency.options,
                                    )
                elif node["kind"] == "symlink":
                    if "artifact" in arg_node:
                        edge = arg_node["artifact"]
                        node["artifact"] = edge + offset if is_number(edge) else edge
                    if "path" in arg_node:
                        node["path"] = arg_node["path"]
                else:
                    raise AssertionError("unreachable")
                nodes.append(node)
            offset += len(arg_nodes)
        return {"nodes": nodes}

    def _encode(self, value):
        return GraphObject.to_data(value)

    def _decode(self, value):
        return GraphObject.from_data(value)

    def _children(self):
        return GraphObject.children(self._value) if self._value is not None else []

    @classmethod
    def expect(cls, value: builtins.object) -> Self:
        assert isinstance(value, cls)
        return value

    @classmethod
    def assert_(cls, value: builtins.object) -> None:
        assert isinstance(value, cls)

    @tg.property
    async def nodes(self, client: Client | None = None) -> list[GraphNode]:
        """Get the graph's nodes in order, loading every leaf of a branched graph."""
        value = await self.load(client)
        if "nodes" in value:
            return cast(GraphLeafValue, value)["nodes"]
        nodes: list[GraphNode] = []
        for child in value["children"]:
            nodes.extend(await self._child_graph(child["graph"]).nodes(client))
        return nodes

    async def node(self, index: int, client: Client | None = None) -> GraphNode | None:
        """Get the node at an index, loading only the leaf that contains it."""
        graph = self
        offset = index
        while True:
            value = await graph.load(client)
            if "nodes" in value:
                nodes = cast(GraphLeafValue, value)["nodes"]
                return nodes[offset] if 0 <= offset < len(nodes) else None
            for child in value["children"]:
                if offset < child["count"]:
                    graph = graph._child_graph(child["graph"])
                    break
                offset -= child["count"]
            else:
                return None

    def _child_graph(self, graph: Graph) -> Graph:
        # Inherit this graph's location and tokens.
        Object.inherit_location(graph, self.state.location)
        Object.inherit_tokens(graph, self.state.tokens)
        return graph

    async def get(
        self, index: int, client: Client | None = None
    ) -> Directory | File | Symlink:
        if not is_number(index) or int(index) != index or index < 0:
            raise AssertionError("invalid graph index")
        index = int(index)
        node = await self.node(index, client)
        if node is None:
            raise AssertionError("invalid graph index")
        artifact = self.pointer(index, node["kind"]).artifact()
        Object.inherit_location(artifact, self.state.location)
        Object.inherit_tokens(artifact, self.state.tokens)
        return artifact

    def pointer(self, index: int, kind: str) -> Pointer:
        return Pointer(self, index, kind)


@dataclass
class Pointer:
    __tangram_atomic__ = True
    graph: Graph
    index: int
    kind: str

    def to_data(self):
        return {"graph": self.graph.id, "index": self.index, "kind": self.kind}

    def to_data_string(self):
        return f"graph={self.graph.id}&index={self.index}&kind={self.kind}"

    @staticmethod
    def is_(value: builtins.object) -> TypeGuard[Pointer]:
        return isinstance(value, Pointer) or (
            isinstance(value, dict)
            and is_number(value.get("index"))
            and isinstance(value.get("kind"), str)
        )

    @classmethod
    def from_arg(cls, arg):
        if isinstance(arg, cls):
            arg = {"graph": arg.graph, "index": arg.index, "kind": arg.kind}
        assert isinstance(arg.get("graph"), Graph), "missing graph"
        check_index(arg.get("index"))
        assert arg.get("kind") is not None, "missing kind"
        return cls(arg["graph"], arg["index"], arg["kind"])

    @classmethod
    def from_data(cls, data):
        if isinstance(data, str):
            return cls.from_data_string(data)
        assert isinstance(data.get("graph"), str), "missing graph"
        assert data.get("kind") is not None, "missing kind"
        check_index(data.get("index"))
        return cls(Graph.with_id(data["graph"]), data["index"], data["kind"])

    @classmethod
    def from_data_string(cls, data):
        params = {}
        for param in data.split("&"):
            pieces = param.split("=")
            if len(pieces) < 2:
                raise ValueError("missing value")
            key, value = pieces[:2]
            if key not in ("graph", "index", "kind"):
                raise ValueError("invalid key")
            params[key] = decode_uri_component(value)
        assert "index" in params, "missing index"
        assert "kind" in params, "missing kind"
        assert "graph" in params, "missing graph"
        try:
            value = params["index"].strip()
            index = (
                int(value, 0)
                if value.lower().startswith(("0x", "0o", "0b"))
                else float(value or "0")
            )
        except ValueError:
            raise AssertionError("invalid node index") from None
        check_index(index)
        return cls(Graph.with_id(params["graph"]), int(index), params["kind"])

    @staticmethod
    def children(object_):
        return [object_.graph]

    def artifact(self):
        from .directory import Directory
        from .file import File
        from .symlink import Symlink

        types = {"directory": Directory, "file": File, "symlink": Symlink}
        return types[self.kind](value=self)


class GraphBuilder(Builder[Graph]):
    type = Graph

    def __init__(self, *args: GraphInput, client: Client | None = None) -> None:
        super().__init__(*args, client=client)

    def node(self, node: GraphNodeInput) -> Self:
        return self._push({"nodes": [node]})

    def nodes(self, nodes: Unresolved[list[GraphNodeInput] | None]) -> Self:
        return self._push({"nodes": nodes})


setattr(Graph, "Builder", GraphBuilder)

graph = GraphBuilder


def optional(value):
    return None if value is UNSET else value


def is_number(value):
    return type(value) in (int, float)


def check_index(value):
    assert (
        is_number(value)
        and math.isfinite(value)
        and value >= 0
        and value <= 2**53 - 1
        and int(value) == value
    ), "invalid node index"


def as_dependency(value):
    if isinstance(value, Referent):
        return value
    if (
        is_number(value)
        or isinstance(value, (Object, Pointer))
        or (isinstance(value, dict) and "index" in value)
    ):
        return Referent(value)
    return Referent(value["node"], value.get("options") or {})


class Edge:
    @staticmethod
    def from_arg(arg, nodes=None):
        if is_number(arg):
            check_index(arg)
            assert nodes is not None and arg < len(nodes), "invalid node index"
            return int(arg)
        if ArgPointer.is_(arg):
            return Pointer.from_arg(arg)
        return arg

    @staticmethod
    def to_data(object_, f=lambda node: node.id):
        if is_number(object_):
            return object_
        if Pointer.is_(object_):
            return Pointer.from_arg(object_).to_data()
        return f(object_)

    @staticmethod
    def from_data(data, f=Object.with_id):
        if is_number(data):
            check_index(data)
            return int(data)
        if isinstance(data, str):
            return Edge.from_data_string(data, f)
        if GraphDataPointerNamespace.is_(data):
            try:
                return Pointer.from_data(data)
            except (ValueError, AssertionError, KeyError, TypeError):
                pass
        return f(data)

    @staticmethod
    def to_data_string(object_, f=lambda node: node.id):
        if is_number(object_):
            return str(int(object_)) if int(object_) == object_ else str(object_)
        if Pointer.is_(object_):
            return Pointer.from_arg(object_).to_data_string()
        return f(object_)

    @staticmethod
    def from_data_string(data, f=Object.with_id):
        if re.fullmatch(r"[0-9]+", data):
            index = int(data)
            check_index(index)
            return index
        if "index=" in data:
            return Pointer.from_data_string(data)
        return f(data)

    @staticmethod
    def children(object_):
        if Pointer.is_(object_):
            return [Pointer.from_arg(object_).graph]
        return [object_] if isinstance(object_, Object) else []


class Dependency:
    @staticmethod
    def to_data_string(value):
        value = as_dependency(value)
        return value.without_token().to_data_string(
            lambda node: "" if node is None else Edge.to_data_string(node)
        )

    @staticmethod
    def from_data_string(data):
        query = data.split("?", 2)
        if len(query) > 1:
            for param in query[1].split("&"):
                if param.split("=", 1)[0] not in (
                    "artifact",
                    "id",
                    "location",
                    "name",
                    "path",
                    "tag",
                ):
                    raise ValueError("invalid key")
        return Referent.from_data_string(
            data, lambda node: Edge.from_data_string(node) if node else None
        )


class GraphObject:
    @staticmethod
    def to_data(object_: GraphValue) -> GraphData:
        if "nodes" in object_:
            nodes = cast(GraphLeafValue, object_)["nodes"]
            return {"nodes": [Node.to_data(node) for node in nodes]}
        children = object_["children"]
        return {
            "children": [
                {"count": child["count"], "graph": child["graph"].id}
                for child in children
            ]
        }

    @staticmethod
    def from_data(data: GraphData) -> GraphValue:
        if "nodes" in data:
            nodes = cast(GraphDataLeaf, data)["nodes"]
            return {"nodes": [Node.from_data(node) for node in nodes]}
        children = data["children"]
        return {
            "children": [
                {"count": child["count"], "graph": Graph.with_id(child["graph"])}
                for child in children
            ]
        }

    @staticmethod
    def children(object_: GraphValue) -> list[Object]:
        if "nodes" in object_:
            nodes = cast(GraphLeafValue, object_)["nodes"]
            return [child for node in nodes for child in Node.children(node)]
        children = object_["children"]
        return [child["graph"] for child in children]


class Node:
    @staticmethod
    def is_directory(node):
        return node["kind"] == "directory"

    @staticmethod
    def to_data(object_):
        return {"kind": object_["kind"], **node_namespace(object_).to_data(object_)}

    @staticmethod
    def from_data(data):
        return {"kind": data["kind"], **node_namespace(data).from_data(data)}

    @staticmethod
    def children(node):
        return node_namespace(node).children(node)


def node_namespace(node):
    namespaces = {
        "directory": GraphDirectory,
        "file": GraphFile,
        "symlink": GraphSymlink,
    }
    assert node["kind"] in namespaces, "unreachable"
    return namespaces[node["kind"]]


class GraphDirectory:
    @staticmethod
    def is_leaf(directory):
        return "entries" in directory

    @staticmethod
    def is_branch(directory):
        return "children" in directory

    @staticmethod
    def to_data(object_):
        if GraphDirectory.is_leaf(object_):
            return {
                "entries": {
                    name: Edge.to_data(edge)
                    for name, edge in object_["entries"].items()
                }
            }
        return {
            "children": [
                {
                    "directory": Edge.to_data(child["directory"]),
                    "count": child["count"],
                    "last": child["last"],
                }
                for child in object_["children"]
            ]
        }

    @staticmethod
    def from_data(data):
        from .artifact import Artifact
        from .directory import Directory

        if GraphDataDirectoryNamespace.is_branch(data):
            return {
                "children": [
                    {
                        "directory": Edge.from_data(
                            child["directory"], Directory.with_id
                        ),
                        "count": child["count"],
                        "last": child["last"],
                    }
                    for child in data["children"]
                ]
            }
        return {
            "entries": {
                name: Edge.from_data(edge, Artifact.with_id)
                for name, edge in data["entries"].items()
            }
        }

    @staticmethod
    def children(object_):
        edges = (
            object_["entries"].values()
            if GraphDirectory.is_leaf(object_)
            else [child["directory"] for child in object_["children"]]
        )
        return [child for edge in edges for child in Edge.children(edge)]


class GraphFile:
    @staticmethod
    def to_data(object_):
        data = {"contents": object_["contents"].id}
        if object_["dependencies"]:
            data["dependencies"] = {
                reference: None
                if dependency is None
                else Dependency.to_data_string(dependency)
                for reference, dependency in object_["dependencies"].items()
            }
        if object_["executable"] is not False:
            data["executable"] = object_["executable"]
        if object_["module"] is not None:
            data["module"] = object_["module"]
        return data

    @staticmethod
    def from_data(data):
        from .blob import Blob

        assert data.get("contents") is not None
        return {
            "contents": Blob.with_id(data["contents"]),
            "dependencies": {
                reference: None
                if dependency is None
                else Dependency.from_data_string(dependency)
                if isinstance(dependency, str)
                else Referent.from_data(
                    dependency,
                    lambda node: Edge.from_data(node) if node is not None else None,
                )
                for reference, dependency in data.get("dependencies", {}).items()
            },
            "executable": data.get("executable") or False,
            "module": data.get("module"),
        }

    @staticmethod
    def children(object_):
        return [
            object_["contents"],
            *[
                child
                for dependency in object_["dependencies"].values()
                if dependency is not None and dependency.node is not None
                for child in Edge.children(dependency.node)
            ],
        ]


class GraphSymlink:
    @staticmethod
    def to_data(object_):
        return {
            **(
                {"artifact": Edge.to_data(object_["artifact"])}
                if object_.get("artifact") is not None
                else {}
            ),
            **({"path": object_["path"]} if object_.get("path") is not None else {}),
        }

    @staticmethod
    def from_data(data):
        from .artifact import Artifact

        return {
            "artifact": Edge.from_data(data["artifact"], Artifact.with_id)
            if data.get("artifact") is not None
            else None,
            "path": data.get("path"),
        }

    @staticmethod
    def children(object_):
        return (
            Edge.children(object_["artifact"])
            if object_.get("artifact") is not None
            else []
        )


class ArgPointer:
    @staticmethod
    def is_(value):
        return isinstance(value, Pointer) or (
            isinstance(value, dict) and is_number(value.get("index"))
        )


class Arg:
    Object = GraphArgObject
    Node = GraphNodeInput
    DirectoryNode = DirectoryNodeArg
    FileNode = FileNodeArg
    SymlinkNode = SymlinkNodeArg
    Directory = dict
    Dependency = Referent | Object | Pointer | int | dict | None
    File = dict
    Symlink = dict
    Edge = Object | Pointer | int
    Pointer = ArgPointer


class GraphDataEdgeNamespace:
    @staticmethod
    def children(data):
        if is_number(data) or (isinstance(data, str) and re.fullmatch(r"[0-9]+", data)):
            return []
        if isinstance(data, str) and "index=" not in data:
            return [data]
        return GraphDataPointerNamespace.children(data)


class GraphDataPointerNamespace:
    @staticmethod
    def is_(value):
        return isinstance(value, str) or (
            isinstance(value, dict) and is_number(value.get("index"))
        )

    @staticmethod
    def children(data):
        if isinstance(data, str):
            for param in data.split("&"):
                pieces = param.split("=")
                if pieces[0] == "graph" and len(pieces) > 1:
                    return [decode_uri_component(pieces[1])]
            return []
        return [data["graph"]]


class GraphDataDependencyNamespace:
    @staticmethod
    def children(data):
        node = data.split("?", 1)[0] if isinstance(data, str) else data["node"]
        return (
            GraphDataEdgeNamespace.children(node)
            if node is not None and node != ""
            else []
        )

    @staticmethod
    def without_location_and_tokens(data):
        if isinstance(data, str):
            return Dependency.to_data_string(
                Dependency.from_data_string(data).without_location_and_tokens()
            )
        output = dict(data)
        if "options" in data:
            output["options"] = {
                key: value
                for key, value in data["options"].items()
                if key not in ("location", "tokens")
            }
        return output


class GraphDataDirectoryNamespace:
    is_leaf = staticmethod(GraphDirectory.is_leaf)
    is_branch = staticmethod(GraphDirectory.is_branch)

    @staticmethod
    def children(data):
        edges = (
            [child["directory"] for child in data["children"]]
            if GraphDataDirectoryNamespace.is_branch(data)
            else data["entries"].values()
        )
        return [id for edge in edges for id in GraphDataEdgeNamespace.children(edge)]


class GraphDataDirectoryChildNamespace:
    @staticmethod
    def children(data):
        return GraphDataEdgeNamespace.children(data["directory"])


class GraphDataFileNamespace:
    @staticmethod
    def children(data):
        return [
            *([data["contents"]] if data.get("contents") is not None else []),
            *[
                id
                for dependency in data.get("dependencies", {}).values()
                if dependency is not None
                for id in GraphDataDependencyNamespace.children(dependency)
            ],
        ]

    @staticmethod
    def without_location_and_tokens(data):
        from .reference import Reference

        output = dict(data)
        if "dependencies" in data:
            output["dependencies"] = {
                Reference.without_tokens(reference): None
                if dependency is None
                else GraphDataDependencyNamespace.without_location_and_tokens(
                    dependency
                )
                for reference, dependency in data["dependencies"].items()
            }
        return output


class GraphDataSymlinkNamespace:
    @staticmethod
    def children(data):
        return (
            GraphDataEdgeNamespace.children(data["artifact"])
            if data.get("artifact") is not None
            else []
        )


class GraphDataNodeNamespace:
    @staticmethod
    def children(data):
        return {
            "directory": GraphDataDirectoryNamespace,
            "file": GraphDataFileNamespace,
            "symlink": GraphDataSymlinkNamespace,
        }[data["kind"]].children(data)

    @staticmethod
    def without_location_and_tokens(data):
        return (
            {**GraphDataFileNamespace.without_location_and_tokens(data), "kind": "file"}
            if data["kind"] == "file"
            else dict(data)
        )


class GraphDataNamespace:
    Node = GraphDataNodeNamespace
    Directory = GraphDataDirectoryNamespace
    DirectoryChild = GraphDataDirectoryChildNamespace
    File = GraphDataFileNamespace
    Symlink = GraphDataSymlinkNamespace
    Edge = GraphDataEdgeNamespace
    Pointer = GraphDataPointerNamespace
    Dependency = GraphDataDependencyNamespace
    DirectoryLeaf = GraphDataDirectoryLeaf
    DirectoryBranch = GraphDataDirectoryBranch
    DirectoryNode = GraphDataDirectoryLeafNode | GraphDataDirectoryBranchNode
    FileNode = GraphDataFileNode
    SymlinkNode = GraphDataSymlinkNode

    @staticmethod
    def children(data: GraphData) -> list[str]:
        if "nodes" in data:
            nodes = cast(GraphDataLeaf, data)["nodes"]
            return [
                id for node in nodes for id in GraphDataNodeNamespace.children(node)
            ]
        return [child["graph"] for child in data["children"]]

    @staticmethod
    def without_location_and_tokens(data: GraphData) -> GraphData:
        if "nodes" not in data:
            return data
        nodes = cast(GraphDataLeaf, data)["nodes"]
        return {
            "nodes": [
                GraphDataNodeNamespace.without_location_and_tokens(node)
                for node in nodes
            ],
        }


setattr(Graph, "Arg", Arg)
setattr(Graph, "Object", GraphObject)
setattr(Graph, "Node", Node)
setattr(Graph, "Directory", GraphDirectory)
setattr(Graph, "File", GraphFile)
setattr(Graph, "Dependency", Dependency)
setattr(Graph, "Symlink", GraphSymlink)
setattr(Graph, "Edge", Edge)
setattr(Graph, "Pointer", Pointer)
setattr(Graph, "Data", GraphDataNamespace)
setattr(Graph, "Id", str)
setattr(Graph, "ConstructorArg", GraphConstructorArg)
setattr(Graph, "DirectoryNode", dict)
setattr(Graph, "FileNode", dict)
setattr(Graph, "SymlinkNode", dict)
setattr(Graph, "DirectoryLeaf", dict)
setattr(Graph, "DirectoryBranch", dict)
setattr(Graph, "DirectoryChild", dict)

setattr(Dependency, "Data", str | dict)
