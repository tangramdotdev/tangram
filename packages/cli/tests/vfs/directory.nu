use ../../test.nu *

# A large FUSE directory is enumerated over multiple stable snapshot pages.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let source = mktemp --directory
for index in 0..<1000 {
	'data' | save ($source | path join $'file_($index | fill --alignment right --character 0 --width 4).txt')
}
let expected = ls $source | get name | each { path basename } | sort

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
	let path = $server_path | path join 'artifacts' $id
	let first = ls $path | get name | each { path basename } | sort
	let second = ls $path | get name | each { path basename } | sort

	assert ($first == $expected)
	assert ($second == $expected)
}
