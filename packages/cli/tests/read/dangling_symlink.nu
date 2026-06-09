use ../../test.nu *

# Reading a symlink with only a path target fails because it cannot be resolved.

let server = spawn

let link = tg put 'tg.symlink({ "path": "nowhere" })' | str trim

let output = tg read $link | complete
failure $output
assert ($output.stderr | str contains "cannot resolve a symlink with no artifact") "the error should mention the unresolvable symlink"
