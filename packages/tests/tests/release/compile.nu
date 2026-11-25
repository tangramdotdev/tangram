use ../../util/test.nu *
use ../../util/release.nu

# The command-based release build exports the compiled file and stamps a rebuilt executable after a source edit.
let local = server spawn --name local
let path = release fixture
cd $path
let commit = ^git rev-parse HEAD | str trim
let host = ^rustc -vV | lines | where {$in starts-with 'host: '} | first | str replace 'host: ' ''
let target = $host | str replace '-apple-' '-' | str replace '-unknown-' '-'
let binary = $path | path join release $'tangram_($target)'

let output = tg run .#release -- --stage compile --target $target --target-dir 'target/reused directory' --retry | complete
success $output
assert equal (^$binary | str trim) $commit

"\n// source edit\n" | save --append cli/main.rs
let commit = ^python3 scripts/version.py | str trim
let output = tg run .#release -- --stage compile --target $target --target-dir 'target/reused directory' --retry | complete
success $output
assert equal (^$binary | str trim) $commit
