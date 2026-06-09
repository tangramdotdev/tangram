use ../../test.nu *

# Checking in a path that does not exist fails to canonicalize the path.

let server = spawn

let output = tg checkin /nonexistent/path/here | complete
failure $output
assert ($output.stderr | str contains "failed to canonicalize the path") "the error should mention the failed canonicalization"
