#!/bin/bash
# Minimal reproduction for V8 race condition between LSP and build.
# The crash occurs when TypeScript service and JS runtime V8 isolates run concurrently.
set -e

BIN=${1:-./target/debug/tangram}
DIR=$(mktemp -d)
PKG=$(mktemp -d)

cleanup() { jobs -p | xargs -r kill 2>/dev/null || true; rm -rf "$DIR" "$PKG" 2>/dev/null || true; }
trap cleanup EXIT

printf 'export default async () => 42;\n' > "$PKG/tangram.ts"

# Start server.
$BIN -d "$DIR" serve &>/dev/null &
while ! $BIN -d "$DIR" health &>/dev/null; do sleep 0.1; done

URI="file://$PKG/tangram.ts"

# Start LSP with continuous hover requests to keep TypeScript V8 isolate busy.
(
  printf 'Content-Length: 107\r\n\r\n{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"processId":null,"capabilities":{},"rootUri":null}}'
  printf 'Content-Length: 52\r\n\r\n{"jsonrpc":"2.0","method":"initialized","params":{}}'
  MSG='{"jsonrpc":"2.0","method":"textDocument/didOpen","params":{"textDocument":{"uri":"'"$URI"'","languageId":"tangram-typescript","version":1,"text":"export default async () => 42;\\n"}}}'
  printf 'Content-Length: %d\r\n\r\n%s' ${#MSG} "$MSG"
  i=10; while true; do
    MSG='{"jsonrpc":"2.0","id":'"$i"',"method":"textDocument/hover","params":{"textDocument":{"uri":"'"$URI"'"},"position":{"line":0,"character":'"$((i%30))"'}}}'
    printf 'Content-Length: %d\r\n\r\n%s' ${#MSG} "$MSG"
    ((i++)); sleep 0.02
  done
) | $BIN -d "$DIR" lsp &>/dev/null &

sleep 0.3

# Trigger builds while LSP is active - crash occurs when both V8 isolates are running.
for i in 1 2 3 4 5; do
  echo -n "Build $i: "
  $BIN -d "$DIR" build "$PKG" &>/dev/null && echo "OK" || echo "FAILED"
  kill -0 %1 2>/dev/null || { echo "SERVER CRASHED on build $i"; exit 1; }
done
echo "No crash"
