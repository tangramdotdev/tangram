use ../../test.nu *

# Checking in a file reads dependency metadata split across numbered xattr shards.

let server = server spawn
let directory = artifact {
	dependency: dependency
	input: (file --xattrs {
		"user.tangram.dependencies.0": '["./depen'
		"user.tangram.dependencies.1": 'dency"]'
	} input)
}
let id = tg checkin $directory
let object = tg get --depth 2 --pretty $id
assert ($object | str contains './dependency') 'the sharded dependency was not preserved'
