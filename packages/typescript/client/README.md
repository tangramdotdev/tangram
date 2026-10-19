# Native TypeScript client

`tangram_typescript_client` is Tangram's Rust client for the native TypeScript API. It starts the compiler directly, uses async JSON-RPC over stdio, and services filesystem and resolver callbacks while waiting for replies. When pointed at the native executable, it does not require Node or an embedded JavaScript runtime.

The initial schema targets TypeScript `7.1.0-dev.20260930.4`. It implements the subset needed by `tg check`: initialization, custom module resolvers, synthetic snapshot/program creation, diagnostics, and resource release. The API is unstable; use a compatible nightly and consult Microsoft's `packages/typescript/src/api/proto.generated.ts` and official async client when updating it.

## Enable native checking

Build with the `typescript7` Cargo feature (`cargo build --features typescript7`), install a compatible nightly with `bun install -g typescript@next`, then configure the Tangram server:

```json
{
  "compiler": {
    "check_backend": "typescript7",
    "typescript_executable": "/absolute/path/to/tsc"
  }
}
```

The default check backend is `tsrs`. The `typescript7` feature enables the JavaScript frontend without selecting an execution runtime. The executable defaults to `tsc` on PATH; use an explicit path if another TypeScript installation shadows the nightly. Configure the server that handles checks, and restart it after changing configuration.

The npm-installed `tsc` command is a Node launcher. To avoid Node entirely, set `typescript_executable` to the native binary inside the platform package. For example, the Bun global installation on Apple Silicon macOS contains `$HOME/.bun/install/global/node_modules/@typescript/typescript-darwin-arm64/lib/tsc`. Expand `$HOME` to an absolute path in JSON; the configuration does not perform shell expansion. Both the launcher and the direct native binary were tested.

Only check requests use TypeScript 7. The separate `compiler.typescript_backend` setting selects the editor backend, which defaults to tsrs. Documentation requires the `typescript6` Cargo feature and `typescript_backend: "typescript6"`. Services start lazily. Native startup/API failures are reported without falling back to another backend.

The adapter serves virtual modules and Tangram's bundled declaration library through callbacks, including open-buffer contents. Synthetic paths reversibly encode the token-free module URI in a URL-safe base64 directory, so a module always gets the same path regardless of discovery order. Resolution and loading decode that identity directly; there is no full module registry. A per-check token side map merges authorization tokens and restores them before resolution/loading. Tokens are not encoded in TypeScript paths. Each check creates a fresh program; incremental reuse is not implemented. Import attributes are not supplied by the upstream resolver callback and are not supported by this proof of concept. Use ordinary imports for now.

## Validation

Transport unit tests do not require a native compiler or Tangram server:

```nu
cargo test -p tangram_typescript_client
```

The CLI smoke test is opt-in because it requires a compatible external compiler:

```nu
with-env {
    TANGRAM_TEST_TYPESCRIPT7_EXECUTABLE: ($env.HOME | path join '.bun/install/global/node_modules/@typescript/typescript-darwin-arm64/lib/tsc')
} {
    nu packages/cli/test.nu --no-cloud --typescript-backend typescript6 typescript7
}
```

It checks successful ordinary imports, Tangram declarations, dependency edits, Unicode diagnostics, and missing imports. The missing-executable test always runs and verifies that LSP diagnostics still work through TypeScript 6 even when the configured native executable is unavailable.

Requests are serialized through a mutable client. After canceling or failing a request, discard that client: unread messages may remain. The compiler service does this automatically and kills the child on drop; successful checks release their snapshot and resolver before reusing the process.
