use ../lib/test.nu *

# A reference with a get option resolves a subpath within a directory, including nested entries.

let local = server spawn

let dir = tg put --no-tokens 'tg.directory({ "hello.txt": tg.file("hello"), "sub": tg.directory({ "inner.txt": tg.file("inner") }) })' | referent node

let output = tg get $"($dir)?get=hello.txt" | complete
success $output
assert ($output.stdout | str starts-with "tg.file(") "the top-level entry should resolve to a file"

let output = tg get $"($dir)?get=sub/inner.txt" | complete
success $output
assert ($output.stdout | str starts-with "tg.file(") "the nested entry should resolve to a file"
