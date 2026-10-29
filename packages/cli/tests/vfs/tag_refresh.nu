use ../lib/test.nu *
use ../lib/vfs.nu

# Refreshing a tag updates the corresponding VFS view.

if $nu.os-info.name == 'macos' {
	skip_test 'FSKit does not support invalidating externally changed items'
}
vfs skip_unless_supported

# Tag paths resolve their current target and disappear after deletion without remounting the VFS.

let server_path = mktemp --directory
let local = server spawn --directory $server_path --config { vfs: true }
vfs assert_mounted $server_path

let path = vfs store_path $server_path | path join 'dep/tangram.ts'
assert (not ($path | path exists)) 'expected the tag to be absent initially'

let initial = artifact {
	tangram.ts: 'export default () => "initial";'
}
tg tag dep $initial

assert ((open $path) =~ 'initial') 'expected the initial tag target'

let updated = artifact {
	tangram.ts: 'export default () => "updated";'
}
tg tag --force dep $updated
assert ((open $path) =~ 'updated') 'expected the updated tag target'

tg tag delete dep | ignore
assert (not ($path | path exists)) 'expected the deleted tag to disappear'

# A miss in an existing named directory must not hide a new tag.
tg tag -p group/initial $initial
let group = vfs store_path $server_path | path join 'group'
assert ((open ($group | path join 'initial/tangram.ts')) =~ 'initial') 'expected the existing nested tag'
let path = $group | path join 'added/tangram.ts'
assert (not ($path | path exists)) 'expected the nested tag to be absent initially'
tg tag group/added $updated
assert ((open $path) =~ 'updated') 'expected the new nested tag to appear'
