use ../../util/test.nu *
use ../../util/release.nu

# The compile stage stages the executable that the sandboxed build makes, stamped with the commit, and builds it again after a source edit.
let local = server spawn --name local
let path = release fixture
cd $path
let commit = ^git rev-parse HEAD | str trim
let target = release host-target
let binary = $path | path join release $'tangram_($target)'

let output = tg run .#release -- --stage compile --target $target --retry | complete
success $output
assert equal (^$binary | str trim) $commit

"\nedit\n" | save --append source.txt
let commit = ^python3 scripts/version.py | str trim
let output = tg run .#release -- --stage compile --target $target --retry | complete
success $output
assert equal (^$binary | str trim) $commit
