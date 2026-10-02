# Python client typing and JS alignment

The Python client keeps the JS module boundaries and public operation names,
with snake_case spellings and Python awaitable builders. Runtime resolution is
recursive: futures can occur inside records and containers, shared awaitables
are captured once by builders, and atomic Tangram handles remain intact.
Value unions use `Mutation[Any]` as an existential payload: accepting every
mutation payload is necessary because `Mutation[T]` both consumes and produces T.
Typed mutation factories and application retain their concrete T.
Python futures/tasks are reusable. Bare coroutines remain one-shot outside
builder capture; use a task/future when sharing an async result across independent
operations. Builders retain reusable coroutine inputs across repeated awaits.
Annotations use explicit recursive input unions, TypedDict records, tagged wire
unions, generic results, Self, ParamSpec, and overloads where Python supports them.

`Value.Type`, `Value.Input`, and `Value.Data.Type` distinguish resolved values,
recursively future-bearing inputs, and serialized values. Domain input records
provide the same distinction for builders. `Unresolved[T]` describes an outer
awaitable; it does **not** map every field of arbitrary T. `Resolved[T]` is an
identity alias for an already known result type, **not** an inferred deep
transformation. `resolve` overloads preserve scalar and atomic types and resolve
homogeneous mappings/sequences; its dynamic fallback cannot preserve every
arbitrary record shape.

## JS type logic that Python cannot fully express today

| JS source | Type logic | Current Python representation and limit |
| --- | --- | --- |
| `resolve.ts` | Recursive `Unresolved<T>`, `Resolved<T>`, command/function conversion and mapped argument tuples | Explicit domain input unions and overloads. Deep TypedDict field transformations and heterogeneous tuple results are not computed. Standalone Python callable-to-command conversion awaits the embedded runtime. |
| `util.ts` | `MutationMap<T>`, `MaybeMutationMap<T>`, `ValueOrMaybeMutationMap<T>` select fields, remove undefined and make keys optional | Homogeneous map aliases plus concrete domain TypedDicts. These aliases take field value types; they do not transform an arbitrary record. |
| `util.ts` | `UnresolvedArgs<T>`, `ResolvedArgs<T>` map tuple positions; `ReturnValue<T>` conditionally admits void for null | Homogeneous argument containers and explicit result aliases. Python None represents both implicit return and Tangram null. |
| `mutation.ts` | `Arg<T>` and `Inner<T>` conditionally allow array/template/map variants | Discriminated records and typed factory overloads. A generic T cannot eliminate incompatible variants of the inner union. Scalar and outer-future set factories preserve their result; nested future inputs have a broader ValueType result. |
| `command.ts` | Parameter-position mapped unresolved arguments and computed return values | Generic command output and callable signatures where available. Python cannot compute recursively unresolved parameter records from arbitrary callable types. |
| `process.ts`, `process/*.ts` | Builder mode controls run/spawn/exec await result | Mode literals and overloads model this without conditional types; function metadata and exact heterogeneous command arguments remain broader. |
| `graph.ts` | Generic `Edge<T>` unions and tagged node records | These are expressible with ordinary generic unions and TypedDicts; they are not a conditional-type limitation. Concrete edge annotations must preserve object kinds. |

These are typing limitations, not reasons to remove runtime resolution.
[PEP 827](https://peps.python.org/pep-0827/)
may eventually provide the missing type operators; the aliases and explicit
input/result boundaries provide places to adopt them when Python and checkers
support the final specification.

## Verification and detailed audit notes

`check_types.py` checks consumer fixtures with `typing.assert_type`, and verifies
that negative fixtures produce exactly the expected diagnostic rules and lines.
It is part of `bun run check`. Runtime tests include matching JS/Python cases for
value encoding, future resolution, tagged/raw templates, mutation kinds,
artifact proofs, placeholders, referents, and locations.
CLI Python tests exercise the installed Python client against Tangram.

Family-level coverage and remaining boundaries are recorded in
[typing-artifacts.md](typing-artifacts.md),
[typing-process.md](typing-process.md), and
[typing-transport.md](typing-transport.md). Dynamic JSON, native wire decoding,
and heterogeneous dispatch require validation or casts at their boundaries;
these should not erase known public schemas. Any ordinary missing annotation or
schema in the detailed notes remains work, rather than a conditional-type gap.
