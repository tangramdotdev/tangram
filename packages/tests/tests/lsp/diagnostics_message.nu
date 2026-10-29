use ../../lib/test.nu *
use ../../lib/lsp.nu

# Checking and editor diagnostics preserve the same complete, indented TypeScript message chain.

let local = server spawn
let source = "const source = { nested: { count: 'wrong' } };\nexport const value: { nested: { count: number } } = source;"
let path = artifact {'main.tg.ts': $source}
let uri = lsp uri ($path | path join main.tg.ts)
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $uri $source)
    (lsp diagnostics 10 $uri)
]
let diagnostics = (lsp result $responses 10).items | where severity == 1
assert equal ($diagnostics | length) 1
let message = $diagnostics.0.message
snapshot $message '
    Type '{ nested: { count: string; }; }' is not assignable to type '{ nested: { count: number; }; }'.
      The types of 'nested.count' are incompatible between these types.
        Type 'string' is not assignable to type 'number'.
'
let output = tg check ($path | path join main.tg.ts) | complete
failure $output
assert ($output.stderr | str contains $message)
