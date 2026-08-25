use ../../test.nu *

# The ReadWrite FUSE transport mounts the store directory and serves a materialized artifact.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let server_path = mktemp --directory
let server = server spawn --directory $server_path --config {
	vfs: {
		kind: 'fuse'
		io: 'read_write'
		passthrough: 'disabled'
	}
}

^mountpoint -q ($server_path | path join 'store')
assert ($env.LAST_EXIT_CODE == 0) 'expected the store path to be mounted as a VFS'

let source = artifact {
	file.txt: 'hello'
}
let id = tg checkin $source | str trim
let path = $server_path | path join 'store' $id
let contents = open ($path | path join 'file.txt')
assert ($contents == 'hello')

let write = ^sh -c 'printf changed > "$1"' sh ($path | path join 'file.txt') | complete
assert ($write.exit_code != 0) 'expected the VFS mount to reject writes'
