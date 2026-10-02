use ../lib/test.nu *

# Reading a symlink with only a path target fails because it cannot be resolved.

let local = server spawn

let link = tg put --no-tokens 'tg.symlink({ "path": "nowhere" })' | referent node

let output = tg read $link | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> cannot resolve a symlink with no artifact

'
