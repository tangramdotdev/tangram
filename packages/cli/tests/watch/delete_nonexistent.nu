use ../../test.nu *

# Deleting a watch on a path that has no watch fails with a missing-watch error.

let server = spawn

let path = artifact 'test'

let output = tg watch delete $path | complete
failure $output
assert ($output.stderr | str contains 'failed to find the watch') "the error should mention the missing watch"
