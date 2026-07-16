use ../../test.nu *

# The io_uring FUSE transport registers every kernel queue before serving the mount.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let server_path = mktemp --directory
let server = spawn --directory $server_path --config {
	vfs: {
		kind: 'fuse'
		io: 'io_uring'
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
