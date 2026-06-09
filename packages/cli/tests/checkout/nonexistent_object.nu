use ../../test.nu *

# Checking out an artifact by a well-formed id that does not exist fails.

let server = spawn

let path = ($env.TMPDIR? | default '/tmp') | path join 'checkout_nonexistent'

let output = tg checkout dir_0000000000000000000000000000 $path | complete
failure $output
assert ($output.stderr | str contains 'failed to check out the artifact') "the error should mention the failed checkout"
