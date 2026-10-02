# Command and process typing

Python preserves command output types with `Command[A, O]`, `CommandBuilder[O]`, and `Process[O]`. `Builder[M, O]` uses overloads for the TypeScript conditional output: awaiting `run` returns `O`, awaiting `spawn` returns `Process[O]`, and `exec` returns `Never`. Changing modes preserves output types. Fluent setters return `Self`. `Command.Value` retains its payload type; a descriptor preserves both the class `value(...)` constructor and the instance `value` field, which Python otherwise conflates.

`CommandArgObject` and process `ArgObject` explicitly admit awaitables within supported fields, including argument arrays. `ValueInput` provides the recursive value approximation. Resolved command executables, command wire values, inline process command data, generic process outcomes, stdio chunks/events/completions and arguments have separate types. Reader and Writer methods retain byte/string/length types. Namespaces expose their actual types instead of `Any` where implemented.

## Type logic that Python cannot currently reproduce

- TypeScript `UnresolvedArgs<ResolvedArgs<A>>` transforms every argument in a variadic tuple, and a builder call consumes that tuple to produce an empty tuple. Python preserves `O`, but does not claim that its `A` parameter performs this transformation. Function commands still require the future runtime; their signature inference is not advertised.
- `ResolvedReturnValue<O>` maps a maybe-promise `void` result to null, or applies the deep resolved transformation to an unresolved Tangram value. Standalone Python factories cannot infer a callable result because callable commands are not supported. An explicitly typed command preserves its output type through process factories.
- `ValueOrMaybeMutationMap<T>`, `Omit`, indexed access and recursive `Unresolved<T>` cannot be generated for arbitrary structured types. Argument field types are declared explicitly; `FieldInput[T]` expresses outer awaitables and mutations, while `ValueInput` supports nested value futures. These are the replacement points for future conditional/type-construction support rather than an identity alias pretending to implement the transformation.
- TypeScript's environment mapper changes a builder's environment parameter `E` independently of its result and argument tuple. Python currently preserves fluent identity but does not track this additional parameter or infer mapper-specific environment inputs.
- Python 3.12 TypedDicts are open, so domain structures are not automatically subtypes of the recursive value mapping type. Structured location mutations use the unspecialized `Mutation` alternative; closed TypedDict support could narrow this further.

## Dynamic boundaries

Argument schemas and fluent scalar setters are explicit; command creation and command/process builder keyword arguments use `Unpack` for known fields. Sandbox mount, network, port, debugging and terminal arguments use concrete domain schemas. Reducer callbacks operate on heterogeneous field values; their argument-array casts follow the normalization performed by the command argument mapper. Parser entry points retain some dynamic inputs because they decode protocol data rather than validate a Python constructor call. This is distinct from the unavailable generic type transformations described above.

PEP 827 would allow reconsidering the explicitly expanded argument schemas and tuple/output transformations when its syntax and checker support are available. Runtime resolution remains centralized and unchanged by these annotation approximations.

## Behavior checked during this pass

- Tagged command templates now use the process context shell and preserve template components, matching the JS command factory.
- Tagged build templates now use that same context and executable validation.
- Numeric process handles send signals through the host; cancellation stops and waits for an installed stopper, or sends TERM when no stopper exists.
- Existing tests cover shared/nested futures, builder reuse, mapper capture, JS command argument encoding, build validation across mode changes, process outcomes, stream cursors and stream writing. Consumer fixtures separately test inferred results and rejected inputs.

## Additional modules reviewed

| Modules | Typing and behavior |
| --- | --- |
| `module.py` | Explicit source/referent, constructor, wire module and source-location schemas; proof-bearing codecs preserve their existing behavior. |
| `error.py` | Generic awaitable builder, typed fluent fields and getters, recursive wire/resolved errors, locations and internal/module file discriminants; namespace types replace unstructured placeholders. |
| `diagnostic.py` | Separate resolved and optional-location wire TypedDicts; module location round trips retain their source semantics. |
| `progress.py` | Discriminated diagnostic/indicator/log/output union; conversion retains its result type and `last_output` returns that type or null. |
| `queue.py` | Generic iterator results distinguish completed iteration from a payload, preserving cancellation and buffered-value ordering. |
| `stop.py`, `sleep.py` | Explicit reusable `Awaitable[None]` completion and no-result sleep. |

Specialized environment getter typing distinguishes `await command.env`, `await command.env()`, and `await command.env(name)` without changing runtime behavior. Process environment calls have corresponding overloads. Positive and negative consumer fixtures cover these alongside progress, queue, metadata, error builders, process result modes and unresolved fields.
