use ../../test.nu *

# Putting a process with input that is not valid process data fails with a deserialization error.

let server = spawn

let output = tg put --id pcs_010000000000000000000000000000000000000000000000000000 "not json" | complete
failure $output
assert ($output.stderr | str contains "failed to deserialize the data") "the error should mention the deserialization failure"
