use ../../test.nu *

# A reference with a get option that names an entry the directory does not contain fails.

let server = spawn

let dir = tg put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim

let output = tg get $"($dir)?get=nope.txt" | complete
failure $output
assert ($output.stderr | str contains "failed to get the reference") "the error should mention the failed get"
