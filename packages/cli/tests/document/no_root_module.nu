use ../../test.nu *

# Documenting a directory without a root module fails.

let server = spawn

let dir = mktemp --directory
"hello" | save ($dir | path join readme.txt)

let output = tg document $dir | complete
failure $output
assert ($output.stderr | str contains "failed to find a root module") "the error should mention the missing root module"
