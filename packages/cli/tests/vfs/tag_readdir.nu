use ../../test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# Listing the VFS exposes the immediate components of visible tags and reflects deletions.

let server_path = mktemp --directory
let server = spawn --directory $server_path --config { vfs: true }
vfs assert_mounted $server_path

let artifact = artifact {
	tangram.ts: 'export default () => "test";'
}
tg tag -p foo/bar $artifact
tg tag -p foo/baz/qux $artifact

let root = vfs store_path $server_path
assert ('foo' in (ls $root | get name | path basename)) 'expected the root tag component'
assert ((ls ($root | path join 'foo') | get name | path basename | sort) == ['bar', 'baz']) 'unexpected tag entries'
assert ((ls ($root | path join 'foo/baz') | get name | path basename) == ['qux']) 'unexpected nested tag entries'

tg tag delete foo/bar | ignore
assert ((ls ($root | path join 'foo') | get name | path basename) == ['baz']) 'expected the deleted tag to disappear from enumeration'
