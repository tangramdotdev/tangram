use ../../test.nu *

# Input that is not an id in either text or binary form is rejected.

let output = tg id "not_an_id" | complete
failure $output
assert ($output.stderr | str contains "invalid id") "the error should mention the invalid id"

let output = "" | tg id | complete
failure $output
assert ($output.stderr | str contains "invalid id") "empty input should be rejected"
