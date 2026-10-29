use ../../util/test.nu *
use ../../util/vfs.nu

# A group appears in the VFS as soon as it is created, and a tag inside it as soon as it is put.

if $nu.os-info.name == 'macos' {
	skip_test 'FSKit does not support invalidating externally changed items'
}
vfs skip_unless_supported

let server_path = mktemp --directory
let local = server spawn --directory $server_path --config { vfs: true }
vfs assert_mounted $server_path

let group = vfs store_path $server_path | path join 'team'
assert (not ($group | path exists)) 'expected the group to be absent initially'
tg group create team | ignore
assert (($group | path type) == 'dir') 'expected the new group to appear as a directory'

let artifact = artifact {
	tangram.ts: 'export default () => "member";'
}
tg tag team/member $artifact
assert ((open ($group | path join 'member/tangram.ts')) =~ 'member') 'expected the new tag in the group to appear'
