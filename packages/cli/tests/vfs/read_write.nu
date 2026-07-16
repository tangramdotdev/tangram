use ../../test.nu *

# The ReadWrite FUSE transport mounts the artifacts directory and serves a materialized artifact.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let server_path = mktemp --directory
let server = spawn --directory $server_path --config {
	vfs: {
		kind: 'fuse'
		io: 'read_write'
		passthrough: 'disabled'
	}
}

^mountpoint -q ($server_path | path join 'artifacts')
assert ($env.LAST_EXIT_CODE == 0) 'expected the artifacts path to be mounted as a VFS'

let path = artifact {
	file.txt: 'hello'
}
let contents = open ($path | path join 'file.txt')
assert ($contents == 'hello')
