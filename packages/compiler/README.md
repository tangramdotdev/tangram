# Compiler

The compiler supports an embedded tsrs backend alongside TypeScript 6 and the optional TypeScript 7 subprocess checker. tsrs is the default for checking and editor requests.

## Embedded tsrs

The Cargo workspace uses Git dependencies on [Tangram's tsrs fork](https://github.com/tangramdotdev/tsrs), with the revision pinned in `Cargo.lock`. Building Tangram does not require a sibling tsrs checkout. The fork adds import-attribute-aware module resolution, native language-service preparation for synthetic projects, and a fix for exported-symbol completion details. Its import-attribute patch can be removed when equivalent upstream support is available.

The checker and editor backends can be selected independently in the server configuration. To use TypeScript 6 explicitly:

```json
{
  "compiler": {
    "check_backend": "typescript6",
    "typescript_backend": "typescript6"
  }
}
```

Set only `typescript_backend` to `"typescript6"` to retain TypeScript 6 for editor and documentation requests while using tsrs for `tg check`.

`src/tsrs.rs` owns a separate service with an in-process worker thread. It calls the Rust compiler, project, and `LanguageService` APIs directly; it does not launch a TypeScript process or use tsrs's RPC session. Tangram's typed request/response boundary stays in place. The worker reserves a 512 MiB virtual stack for native checker recursion.

Request adapters in `src/tsrs/` follow the operation names in `packages/typescript/src/`, including separate checking and editor-diagnostic handlers. Declaration, definition, and type definition have explicit entry points. Native infrastructure lives in `project.rs` (persistent snapshots and language services), `host.rs` (the virtual filesystem and native change tracking), `resolver.rs` (the native resolver callbacks), `module.rs` (module identities), and `convert.rs` (shared native response conversions).

Both backends use the compiler's document listing, module loading, source revisions, module resolution, and lockfile-version APIs. The native host translates those versions into snapshot updates and tracks which versions its own cache has consumed. Diagnostic conversion follows the same missing-module message policy, and native checking uses tsrs's diagnostic-chain formatter.

The virtual filesystem exposes bundled declarations and Tangram modules, including open buffers. Module identities and authorization tokens remain separate. The resolver passes attributes from static imports, re-exports, and dynamic imports into Tangram resolution, including distinct attributes for identical specifiers.

The worker retains the editor project, snapshot host, parse cache, current snapshot, and cached native language service across requests. Unchanged queries reuse the same program and language service. Source revisions, open-document roots, and full lockfile timestamps drive updates to the existing synthetic project. Native updates reuse unchanged parsed files; a new program version refreshes the language-service wrapper and its program-specific caches. Completion preparation is retained per file and program version. The host also retries unresolved imports to detect newly created dependencies.

`tg check` uses a separate program for each check, matching the TypeScript 6 check handler. LSP document diagnostics call the retained language service's `provide_diagnostics` method, preserving its validation preferences and source mapping behavior. The adapter converts the returned messages, severities, and ranges into Tangram diagnostics. Editor state remains alive independently. Snapshot leases and native program owners retain their configuration and source allocation regions for their full lifetimes; request-only allocations are released after returning owned responses.

The backend implements checking, document diagnostics, completion and resolve, hover, declaration/definition/type definition, implementation, references, prepare rename and rename, signature help, document/workspace symbols, highlights, folding/selection ranges, inlay hints, semantic tokens, import links, call hierarchy, and native code actions. Positions use the client's negotiated UTF-8 or UTF-16 encoding. Completions preserve engine-supplied replacement ranges and deprecation tags, including completion resolution. Rename preserves each native replacement text, including shorthand-property expansions. Both TypeScript backends use the built-in alias-preserving rename mode: renaming an imported binding changes `oldname` to `oldname as newname`, while renaming its source declaration can update the definition and its consumers. Imported cross-language bindings can acquire local aliases without editing their immutable generated wrappers.

Document symbols use the identifier's name span for selection and the full declaration for their enclosing range in both TypeScript backends. Workspace search retains cross-language wrapper symbols returned by the language services, alongside original source symbols. Wrapper positions point into materialized generated files through the same conversion used by definition requests.

Generated cross-language files are immutable to editor operations: rename, code actions, formatting, and completion requests cannot produce edits inside them. Code actions with edits targeting a foreign-language representation or a generated file are excluded as complete actions. Quick-fix availability follows each backend; the spelling-fix fixture expects TypeScript 6's correction and the native service's current lack of a spelling provider.

The integration has remaining limitations:

- Documentation extraction (`Document` requests) is not ported. Set `typescript_backend` to `"typescript6"` for documentation workflows.
- tsrs currently implements fewer quick fixes than TypeScript 6 (including no spelling correction for `Math.florr`). Import organization and native declaration/interface fixes are exposed; full code-action parity is not established.
- Generated auto-import specifiers need a Tangram-aware rewrite. Exported-symbol auto-import completions and generated auto-import fixes are disabled until then.
- Inlay hints follow the existing default preferences, which leave them disabled.
- Requests run serially on the native worker. An abandoned request can finish in the worker; per-request native cancellation is not yet wired.

Run the repository checks and relevant CLI tests with Nushell:

```nu
bun run format
bun run check
nu packages/cli/test.nu --no-cloud 'check/tsrs|lsp/tsrs'
for backend in [typescript6 tsrs] {
    nu packages/cli/test.nu --no-cloud --typescript-backend $backend '(check|lsp)/'
}
```

Validation on October 19, 2026: repository formatting and the full workspace check pass without Clippy warnings. Running the same 89 check/LSP fixtures with either `--typescript-backend typescript6` or `--typescript-backend tsrs` finishes with 87 passed, two Linux-only VFS skips on macOS, and no failures. The shared rename fixtures verify built-in import aliases, successful checking after applying a cross-language alias rename, wrapper immutability, and multi-document version handling when renaming a source declaration. Explicit backend settings in fixtures take precedence over the selected default.

All 14 tsrs-specific CLI fixtures pass. They cover import attributes and attribute-only edits, authorization tokens, completion resolution, rich rename edits, UTF-8/UTF-16 diagnostics, open/unsaved/closed dependencies, source and resolution invalidation, and retained program reuse across more than 40 queries. The shared diagnostic-message fixture passes with both defaults and verifies that standalone checking and editor diagnostics preserve the same complete, indented message chain.

The tsrs retained-configuration regression verifies unchanged-program and AST reuse, root reconfiguration, and configuration-region release. The fork passed all 322 selected project/language-service/API/LSP tests, native Clippy, and source lint during the lifecycle correction. This structural alignment does not change the fork.
