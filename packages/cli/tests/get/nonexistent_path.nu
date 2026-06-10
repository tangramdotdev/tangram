use ../../test.nu *

# Getting a path whose parent does not exist fails to canonicalize, and getting a missing file in an existing directory fails to check in.

let server = spawn

let output = tg get /nonexistent/deeply/nested | complete
failure $output
assert ($output.stderr | str contains "failed to canonicalize the path") "the error should mention the canonicalization"

let tmp = mktemp --directory
let output = tg get ($tmp | path join "nope") | complete
failure $output
assert ($output.stderr | str contains "failed to check in the path") "the error should mention the checkin"
