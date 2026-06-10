use ../../test.nu *

# The metadata flag prints the object's metadata as an info message alongside the value.

let server = spawn

let file = tg put 'tg.file("metadata")' | str trim
tg index

let output = with-env { TANGRAM_QUIET: "false" } { tg get $file --metadata | complete }
success $output
assert ($output.stdout | str starts-with "tg.file(") "the value should print to stdout"
assert ($output.stderr | str contains '"subtree"') "the metadata should print as an info message"
