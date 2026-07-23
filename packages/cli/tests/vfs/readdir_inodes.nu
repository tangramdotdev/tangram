use ../../test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# Every entry returned by readdir must report a nonzero inode number. GNU make ignores
# directory entries whose inode number is zero, so such entries are invisible to it even
# though they exist.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}
if (which make | is-empty) {
	skip_test 'this test requires make'
}

# Create more entries than fit in the first readdirplus response, so that the tail of the
# listing is served by readdir.
const count = 1000
let source = mktemp --directory
for index in 0..<$count {
	'data' | save ($source | path join $'file_($index | fill --alignment right --character 0 --width 4).txt')
}

let server_path = mktemp --directory
let server = spawn --directory $server_path --config { vfs: true }
vfs assert_mounted $server_path

let id = tg checkin $source | str trim
let path = vfs root $server_path $id

let build_path = mktemp --directory
"all:\n\t@echo $(words $(wildcard " + $path + "/*))\n" | save ($build_path | path join 'Makefile')
let visible = ^make --no-print-directory -C $build_path | str trim | into int

assert ($visible == $count) $'expected make to see all ($count) entries, but it saw ($visible)'
