use ../../test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# Listing the VFS exposes the immediate components of visible tags and reflects deletions.

let server_path = mktemp --directory
let server = server spawn --directory $server_path --config { vfs: true }
vfs assert_mounted $server_path

let artifact = artifact {
	tangram.ts: 'export default () => "test";'
}
let artifact = tg checkin $artifact
tg tag -p foo/bar $artifact
tg tag -p foo/baz/qux $artifact

let root = vfs store_path $server_path
let root_entries = ls $root | get name | path basename
assert ('foo' in $root_entries) 'expected the root tag component'
assert ($artifact not-in $root_entries) 'expected artifacts to be omitted from root enumeration'
assert (($root | path join $artifact) | path exists) 'expected artifacts to remain directly addressable'
assert ((ls ($root | path join 'foo') | get name | path basename | sort) == ['bar', 'baz']) 'unexpected tag entries'
assert ((ls ($root | path join 'foo/baz') | get name | path basename) == ['qux']) 'unexpected nested tag entries'

tg tag delete foo/bar | ignore
assert ((ls ($root | path join 'foo') | get name | path basename) == ['baz']) 'expected the deleted tag to disappear from enumeration'

# Tags for unsupported VFS node kinds remain visible as dangling symlinks.
let blob = tg put 'tg.blob("test")' | str trim
let process_path = artifact {
	tangram.ts: 'export default () => "test";'
}
let process = tg build --detach $process_path | str trim
tg wait $process | ignore
tg tag -p foo/blob $blob
tg tag -p foo/process $process

let entries = ls ($root | path join 'foo') | get name | path basename | sort
assert ($entries == ['baz', 'blob', 'process']) 'expected unsupported tag targets to be enumerated'
let blob_path = $root | path join 'foo/blob'
let process_path = $root | path join 'foo/process'
assert equal (^readlink $blob_path | str trim) $"../($blob)"
assert equal (^readlink $process_path | str trim) $"../($process)"
assert not ($blob_path | path exists) 'expected a tag targeting a blob to be dangling'
assert not ($process_path | path exists) 'expected a tag targeting a process to be dangling'
