# Compiler

The compiler checks and provides editor features for JavaScript and TypeScript with the embedded tsrs backend, and for Python with the embedded ty backend.

## Cargo features

Language frontends, checker/editor backends, and execution runtimes are selected separately. Each backend enables its language frontend; enabling a frontend does not select a backend.

| Purpose | JavaScript and TypeScript | Python |
| --- | --- | --- |
| Frontend | `javascript` | `python` |
| Checker and editor backend | `tsrs` | `ty` |
| Execution runtime (CLI) | `v8`, `quickjs` | `cpython` |

The `javascript` frontend provides OXC parsing, import and metadata analysis, formatting, transpilation, and bundled declarations. The `python` frontend provides Ruff parsing, import and metadata analysis, formatting, module resolution, and object-module generation. Cross-language wrappers require both frontends. The `ty` backend prepares the Python checker library; `cpython` includes the interpreter distribution separately.

The compiler and server expose the frontend and checker/editor features. Runtime features belong to the CLI and runtime crates.

The `cpython` runtime uses the pinned, bundled CPython distribution. It does not discover or link a system Python installation; the checked-in PyO3 configuration suppresses automatic Python linking.

The CLI defaults enable `tsrs`, `ty`, `v8`, and `cpython`, along with LMDB, SQLite, and TLS. QuickJS is opt-in. Requests for a language whose backend was not compiled in report the missing feature.

JavaScript, TypeScript, and Python module kinds, module values, and wire representations remain available in every build. Features control source processing and backends, not the client data model.

For example, these Nushell commands build the CLI with language tooling and local storage, without an execution runtime:

```nu
cargo build --no-default-features --features 'lmdb,sqlite,tsrs,ty'
cargo build --no-default-features --features 'lmdb,sqlite,javascript,python'
```

Run the compiler's frontend and missing-backend regression tests across feature selections with:

```nu
cargo test --package tangram_compiler --no-default-features
for features in [javascript python 'javascript,python' tsrs ty] {
    cargo test --package tangram_compiler --no-default-features --features $features
}
```

## Embedded tsrs

The Cargo workspace uses Git dependencies on [Tangram's tsrs fork](https://github.com/tangramdotdev/tsrs), with the revision pinned in `Cargo.lock`. Building Tangram does not require a sibling tsrs checkout. The fork adds import-attribute-aware module resolution, native language-service preparation for synthetic projects, and a fix for exported-symbol completion details. Its import-attribute patch can be removed when equivalent upstream support is available.

`src/tsrs.rs` owns a service with an in-process worker thread. It calls the Rust compiler, project, and `LanguageService` APIs directly; it does not launch a TypeScript process or use tsrs's RPC session. The worker reserves a 512 MiB virtual stack for native checker recursion.

Request adapters live in `src/tsrs/`. Declaration, definition, and type definition have explicit entry points. Native infrastructure lives in `project.rs` (persistent snapshots and language services), `host.rs` (the virtual filesystem and native change tracking), `resolver.rs` (the native resolver callbacks), `module.rs` (module identities), and `convert.rs` (shared native response conversions). `document.rs` generates documentation directly from the type checker.

The backend uses the compiler's document listing, module loading, source revisions, module resolution, and lockfile-version APIs. The native host translates those versions into snapshot updates and tracks which versions its own cache has consumed. Diagnostic conversion follows the compiler's missing-module message policy, and native checking uses tsrs's diagnostic-chain formatter.

The virtual filesystem exposes bundled declarations and Tangram modules, including open buffers. Module identities and authorization tokens remain separate. The resolver passes attributes from static imports, re-exports, and dynamic imports into Tangram resolution, including distinct attributes for identical specifiers.

The worker retains the editor project, snapshot host, parse cache, current snapshot, and cached native language service across requests. Unchanged queries reuse the same program and language service. Source revisions, open-document roots, and full lockfile timestamps drive updates to the existing synthetic project. Native updates reuse unchanged parsed files; a new program version refreshes the language-service wrapper and its program-specific caches. Completion preparation is retained per file and program version. The host also retries unresolved imports to detect newly created dependencies.

`tg check` uses a separate program for each check. LSP document diagnostics call the retained language service's `provide_diagnostics` method, preserving its validation preferences and source mapping behavior. The adapter converts the returned messages, severities, and ranges into Tangram diagnostics. Editor state remains alive independently. Snapshot leases and native program owners retain their configuration and source allocation regions for their full lifetimes; request-only allocations are released after returning owned responses.

The backend implements checking, documentation, document diagnostics, completion and resolve, hover, declaration/definition/type definition, implementation, references, prepare rename and rename, signature help, document/workspace symbols, highlights, folding/selection ranges, inlay hints, semantic tokens, import links, call hierarchy, and native code actions. Positions use the client's negotiated UTF-8 or UTF-16 encoding. Completions preserve engine-supplied replacement ranges and deprecation tags, including completion resolution. Rename preserves each native replacement text, including shorthand-property expansions, and uses the alias-preserving rename mode: renaming an imported binding changes `oldname` to `oldname as newname`, while renaming its source declaration can update the definition and its consumers. Imported cross-language bindings can acquire local aliases without editing their immutable generated wrappers.

Document symbols use the identifier's name span for selection and the full declaration for their enclosing range. Workspace search retains cross-language wrapper symbols returned by the language service, alongside original source symbols, and their positions point into materialized generated files.

Each generated cross-language wrapper records the name and original range of every export in a marker comment, and carries the export's documentation. Definition, declaration, type definition, implementation, and reference locations inside a wrapper map to the export's name in the original module, and documentation names and locates exports from wrappers by their original declarations.

Generated cross-language files are immutable to editor operations: rename, code actions, formatting, and completion requests cannot produce edits inside them. Code actions with edits targeting a foreign-language representation or a generated file are excluded as complete actions.

The integration has remaining limitations:

- tsrs implements fewer quick fixes than TypeScript, including no spelling correction. Import organization and native declaration/interface fixes are exposed.
- Generated auto-import specifiers need a Tangram-aware rewrite. Exported-symbol auto-import completions and generated auto-import fixes are disabled until then.
- Inlay hints follow the existing default preferences, which leave them disabled.
- Requests run serially on the native worker. An abandoned request can finish in the worker; per-request native cancellation is not yet wired.

Run the repository checks and relevant CLI tests with Nushell:

```nu
bun run format
bun run check
nu packages/cli/test.nu --no-cloud '(check|document|lsp)/'
```
