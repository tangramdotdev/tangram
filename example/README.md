# rusty-v8-real-repl

This is a minimal JavaScript REPL and script runner example with two REPL engines:

- `v8` uses V8 Inspector `Runtime.evaluate` with `replMode: true`.
- `quickjs` uses `rquickjs::Ctx::eval_with_options` with QuickJS-NG's global async eval flags in a persistent QuickJS context.

Run the V8 REPL with:

```sh
cargo run --manifest-path examples/rusty-v8-real-repl/Cargo.toml
cargo run --manifest-path examples/rusty-v8-real-repl/Cargo.toml -- repl v8
```

Run the QuickJS REPL with:

```sh
cargo run --manifest-path examples/rusty-v8-real-repl/Cargo.toml -- repl quickjs
```

Try:

```js
var x = 1
x + 41
await Promise.resolve("ok")
x = 2
```

V8 accepts top-level `let` redeclarations because the example uses inspector REPL mode. QuickJS-NG does not provide an equivalent lexical redeclaration mode, so the QuickJS backend reports the same `SyntaxError` as `qjs` for `let x = 1` followed by `let x = 2`.

Use `.exit` or EOF to quit.

Run a V8 script with:

```sh
cargo run --manifest-path examples/rusty-v8-real-repl/Cargo.toml -- run script.js
```

The script runner executes a classic V8 script in a fresh context. It does not implement module loading, host timers, or top-level await.

Inspector support:

```sh
cargo run --manifest-path examples/rusty-v8-real-repl/Cargo.toml -- run script.js --inspect
cargo run --manifest-path examples/rusty-v8-real-repl/Cargo.toml -- run script.js --inspect=127.0.0.1:9230
cargo run --manifest-path examples/rusty-v8-real-repl/Cargo.toml -- run script.js --inspect-wait
cargo run --manifest-path examples/rusty-v8-real-repl/Cargo.toml -- run script.js --inspect-brk
```

`--inspect` starts a Chrome DevTools Protocol server and runs immediately. `--inspect-wait` waits until a debugger connects before running. `--inspect-brk` waits for a debugger and schedules a pause before the first statement.

Open `chrome://inspect`, configure the printed host and port if needed, then connect to the listed target.
