use ../lib/test.nu *
use ../lib/vfs.nu

# Creating a directory through the VFS makes it visible in the mounted tree.

vfs skip_unless_supported

# Creating a directory in the mounted VFS fails because it is read-only.

let server_path = mktemp --directory
let local = server spawn --directory $server_path --config { vfs: true }
vfs assert_mounted $server_path

let id = tg build (artifact {
	tangram.ts: '
		export default () => tg.directory({ "file.txt": tg.file("contents") })
	'
})

let path = vfs root $server_path $id
vfs assert_read_only (^mkdir ($path | path join 'directory') | complete) 'mkdir'
