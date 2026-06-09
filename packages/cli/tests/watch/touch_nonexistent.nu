use ../../test.nu *

# Touching a watch on a path that has no watch fails.

let server = spawn

let path = artifact 'test'

let output = tg watch touch $path | complete
failure $output
assert ($output.stderr | str contains 'expected a watch') "the error should mention the missing watch"
