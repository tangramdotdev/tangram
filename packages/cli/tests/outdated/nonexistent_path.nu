use ../../test.nu *

# Outdated fails for a path that does not exist.

let server = spawn

let output = tg outdated /nonexistent/path/nowhere | complete
failure $output
assert ($output.stderr | str contains "failed to find the root") "the error should mention the root"
