use ../../test.nu *

# The metadata flag prints the object's metadata as an info message alongside the value.

let server = spawn

let file = tg put 'tg.file("metadata")' | str trim
tg index

let output = with-env { TANGRAM_QUIET: "false" } { tg get $file --metadata | complete }
success $output
assert ($output.stdout | str starts-with "tg.file(") "the value should print to stdout"
snapshot --normalize-ids $output.stderr '
	info fil_010000000000000000000000000000000000000000000000000000
	info {"node":{"size":44,"solvable":false,"solved":true},"subtree":{"count":2,"depth":2,"size":53,"solvable":false,"solved":true}}

'
