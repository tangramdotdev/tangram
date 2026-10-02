# Transport typing and behavior audit

The transport uses resolved arguments. Recursive awaitable resolution belongs to the higher-level builders before they call these endpoints. HTTP framing does not require a conditional or mapped type.

## Coverage

- `client/__init__.py`: typed connection/retry lifecycle, byte/string versus streaming write overloads, blob read event streams, object/process/sandbox results, process connection messages, stdio read streams, and deferred wait results. Generic retry preserves the callback result. Public convenience keyword arguments use concrete `Unpack` dictionary shapes. Private normalization and request forwarding remain dynamic at subsystem boundaries.
- `client/checkin.py`, `client/checkout.py`: separate `ArgObject` dictionary structures from the source-compatible `Arg.to_json` codec namespaces. Checkin options retain Python snake_case fields; the codec also accepts existing JS-style spellings.
- `client/read.py`, `client/write.py`: typed blob read discriminated events and write overloads. Write returns an ID for byte/string input and a referent-bearing output for streaming input.
- `client/object/get.py`, `client/object/put.py`, `client/object/batch.py`: reuse `Object.Get/Put/Batch` output models. Retrieval arguments use `Object.Get.Arg`. Insertion convenience inputs distinguish raw `ObjectWireData` from `Object.Put.Arg`; batch inputs distinguish `Object.Batch.Arg` from its entry list.
- `client/process/get.py`, `client/process/put.py`, `client/process/cancel.py`, `client/process/signal.py`: concrete endpoint dictionaries and results, token maps, and distinct request/response location structures.
- `client/process/spawn.py`: `ArgObject`, `CommandArgObject`, `OutputObject` distinguish dictionary shapes from codec namespaces. The process output is `process: int | str`, with optional cached/lease/outcome/proofs.
- `client/process/connect.py`: tagged request, response, receipt, read-notification, and client/server message unions, with typed stdio data. JSON conversion has an explicit dynamic boundary.
- `client/process/wait.py`: deferred waiting returns a callable producing an awaitable outcome. This matches the JS source, whose `waitProcessPromise` returns `Promise<() => Promise<Outcome | null>>`, rather than a nested Promise. Each invocation performs a new wait.
- `client/process/stdio/read.py`, `client/process/stdio/write.py`: typed connections, channels, reconnect factories, read event streams, and write chunk streams/completion callbacks. Replay/pending state and decoded transport messages retain dynamic dictionaries internally.
- `client/process/tty/put.py`: concrete TTY argument shape shared through `client.process.connect.TtyArg`; scalar result annotations.
- `client/sandbox/create.py`, `client/sandbox/get.py`, `client/sandbox/destroy.py`: typed sandbox outputs and destruction results. Creation uses a separate resolved `DataArgObject`, distinct from unresolved sandbox construction inputs.
- `http/__init__.py`: generic response stream; it closes iterators that expose `aclose` and always releases the response.
- `http/body.py`: overloaded descriptors retain both JS static and instance `json`/`sse` APIs with distinct types; byte collection and SSE events are typed.
- `http/headers.py`, `http/request.py`, `http/response.py`, `http/uri.py`: header values, request arguments/body sources, event-emitting response stream protocol, URI arguments, and recursive covariant query shapes.
- `http2.py`: response header futures, buffered data/error/end union, writer tasks, socket reader/writer, and session connection results.
- `host/__init__.py`, `host/default.py`: HTTP session protocol, OS file descriptor/async stream unions, subprocess pipes, stoppable operations, signal iteration, outcomes, and TTY sizes. Host replacement and native serialization remain dynamic boundaries.

## Gaps and future type logic

- Decoding JSON cannot infer a schema from a caller's desired return type; `Body.json()`/`Response.json()` return `object`, matching JavaScript’s `unknown`. Endpoints cast those decoded values to explicit wire/result schemas at the decoding boundary. A schema/decoder argument would be needed for validated generic decoding. PEP 827 alone does not supply that validation.
- Awaitables nested inside high-level arguments need the central unresolved/resolved abstraction. Low-level endpoint shapes deliberately describe already-resolved data and can be targets for a future recursive resolved transformation.
- Some internal JSON message dispatch and option normalization remain dynamic. Public `Client` convenience keywords now use concrete `Unpack` argument dictionaries.
- Codec namespaces and value types coexist as separate Python names (`Spawn.Arg` versus `Spawn.ArgObject`, for example), since a Python class cannot simultaneously behave as an ordinary namespace and a structural dictionary type. Future type introspection may reduce repetition, but namespace layout should remain source aligned.
- Detailed object wire payloads and command values depend on the artifact/value models; they should remain synchronized with those modules rather than duplicated here.

## Behavior and validation

The audit found HTTP/2 resource leaks when missing responses and successful bodyless responses were abandoned. Blob reads, process retrieval/cancellation/signalling/TTY resizing, and sandbox retrieval/destruction now release those responses; conflict responses are released too. Regression tests exercise all of these paths without draining response bodies.

Consumer fixtures check inferred write results, read results, process outputs, and static/instance HTTP methods; negative fixtures reject invalid write, body, and request arguments. Focused HTTP, client, and host runtime tests and scoped `ty`/`ruff` checks pass.

## Additional utility modules

- `authorization.py`: token bodies and proof-map inputs/results are typed; public `Tokens.inherit`/`Tokens.normalize` return `None`, matching JS mutation-only namespace methods. Internal module convenience helpers retain their mutated dictionary return for existing implementation callers.
- `builtin.py`: artifact/blob inputs and outputs, archive/compression/checksum literals, recursive unresolved bundle input, and download option dictionary. Download now updates the caller's checksum option in place and uses a nullish mode fallback, matching JS; a regression verifies caller option mutation.
- `args.py`: generic argument and mapper input types, awaitable mapping, mutation-kind/custom reducer union, and explicit output dictionary. The JS signature's field-specific `keyof`, indexed access, `Exclude`, conditional mutation eligibility, and mapped reducer object cannot be expressed precisely today. The Python reducer operates on `object`; runtime value validation guards the mutation branch. Future PEP 827 transformations can replace this centralized fallback with field-preserving input/output typing.

Process retrieval/storage data now use the shared `ProcessDataObject` wire schema; metadata uses `object`, matching JS `unknown`. Stdio read helpers return decoded `StdioChunk` streams, while connection notification messages retain the `StdioReadEvent` tagged union.
