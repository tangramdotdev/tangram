use ../../test.nu *

# FUSE reports symlink target size consistently with readlink.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let source = artifact {
	link: (symlink 'target.txt')
	target.txt: 'target'
}

let transports = if (fuse_io_uring_available) {
	['io_uring' 'read_write']
} else {
	['read_write']
}
for io in $transports {
	let server_path = mktemp --directory
	let server = spawn --directory $server_path --config {
		vfs: {
			kind: 'fuse'
			io: $io
			passthrough: 'disabled'
		}
	}
	let id = tg checkin $source | str trim
	let path = $server_path | path join 'artifacts' $id 'link'
	let target = ^readlink $path | str trim
	let size = ^stat --format=%s -- $path | str trim | into int

	assert ($target == 'target.txt')
	assert ($size == ($target | str length))
}
