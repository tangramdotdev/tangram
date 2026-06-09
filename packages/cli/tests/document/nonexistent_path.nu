use ../../test.nu *

# Documenting a path that does not exist fails.

let server = spawn

let output = tg document /nonexistent/path/nowhere | complete
failure $output
assert ($output.stderr | str contains "failed to canonicalize the path") "the error should mention the path"
