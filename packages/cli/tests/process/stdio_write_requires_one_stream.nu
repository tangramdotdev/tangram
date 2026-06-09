use ../../test.nu *

# Writing process stdio without exactly one stream selected fails.

let server = spawn

let output = "data" | tg process stdio write pcs_010000000000000000000000000000000000000000000000000000 | complete
failure $output
assert ($output.stderr | str contains 'expected exactly one stdio stream') "the error should require exactly one stdio stream"
