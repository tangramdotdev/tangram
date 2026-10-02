# Artifact typing and behavior review

Reviewed `artifact.ts`, `blob.ts`, `directory.ts`, `file.ts`, `file/xattrs.ts`,
`graph.ts`, `object.ts`, `symlink.ts`, `sandbox.ts`, `referent.ts`, `reference.ts`,
`path.ts`, and `checksum.ts` against their Python modules.

Implemented concrete awaitable builder results, `Self` fluent return types,
recursive future-bearing input aliases, typed async properties, typed iterator
results, constructor state schemas, graph node discriminators, blob wire data,
object operation arguments/results, and sandbox arguments/data/results. Python
schema names live at module level; namespaces retain their runtime helper classes.
For example, `BlobWireData` describes the value consumed by `Blob.Data`, while
`Blob.Data` remains the namespace containing `children`.

The positive consumer fixture checks inferred factory, builder, getter, and future
input types. The negative fixture checks invalid scalar setter and entry inputs.

Behavior fixes discovered during the typing review:

- Blob builders now handle the same tagged string array, unindent, and raw forms
  as file builders and the JS blob builder.
- Blob, directory, and file constructors accept JS constructor state dictionaries.
- Artifact referents with absent options use empty proofs and no location.
- File data children tolerate absent contents, while decoded file objects still
  require contents as JS does.
- File object children place contents before dependencies regardless of dictionary
  insertion order; file decoding preserves a dependency on graph node zero.
- Compact dependency serialization handles absent referent options without
  iterating a null options value.

## Type logic that Python cannot express generally

`Args<T>` combines recursive `Unresolved<T>`, argument arrays, and
`ValueOrMaybeMutationMap<T>`. The recursive per-artifact input aliases explicitly
represent the accepted shapes. They do not calculate an arbitrary mapped shape
from a user's type parameter. A future type introspection/conditional type system
could replace that duplication while preserving these runtime contracts.

`Graph.Edge<T>` and `MaybeReferent<T>` preserve the payload parameter through
pointer and referent wrappers. Generic `GraphEdge[T]`, `GraphArgEdge[T]`,
`GraphDataEdge[T]`, `Referent[T]`, and `ReferentData[T]` express these wrappers
today. Generic codec callbacks preserve payload types. Future conditional types
would additionally describe graph
edge resolution and raw data transformation, including selecting the artifact
class from a pointer's `kind`.

`MaybeMutation<T>` and `Mutation.Map<T>` select legal mutation operations from the
field type and recursively transform object fields. Current argument schemas
explicitly admit mutations at the fields where JS supports them. The generic
mutation implementation is the hook for finer operation-specific inference.

`resolve` transforms nested awaitables while leaving objects atomic. Python
cannot express this transformation for every arbitrary user-defined shape.
Artifact argument aliases enumerate recursive futures and artifact handles remain
atomic. Concrete builder results and getters do not depend on speculative PEP
syntax or broad `Any` fallback.

## Remaining precision boundaries

Object IDs remain strings, like the JS aliases. Python integers also accept bool
statically; runtime index validation continues to reject booleans. Runtime
namespace helpers cannot simultaneously be a union type under the same name,
so concrete module-level schema aliases complement the helper namespaces.

The complete `ObjectWireData` tagged union retains the corresponding blob,
directory, file, graph, symlink, command, and error payload schemas. Compact graph
edges, pointers, and dependencies have explicit wire unions. Codec parser bodies
retain dynamic values at validation/dispatch boundaries; public aggregate codecs
carry concrete input/output contracts. Future type logic could relate the wrapper
kind and payload automatically, but no speculative PEP syntax is required.

One current checker inference limit appears with overloaded builtins used as
callbacks for generic TypedDict codecs: `from_data(data, int)` can infer an overly
broad input parameter. A concrete `def decode_int(value: str) -> int` preserves
both the generic node type and result. The consumer fixture checks that case.
This is checker inference work rather than missing Python conditional types.

Conditional type support will not by itself make these checks stronger. Explicit
argument models, generic results, namespace contracts, and positive/negative
consumer fixtures are the current foundation and should remain regression tests.
