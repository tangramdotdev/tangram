use ../../test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# Creating a file in the mounted VFS and writing to an existing one both fail because it is read-only.

let server_path = mktemp --directory
let server = spawn --directory $server_path --config { vfs: true }
vfs assert_mounted $server_path

let id = tg build (artifact {
	tangram.ts: '
		export default () => tg.directory({ "file.txt": tg.file("contents") })
	'
})

let path = vfs root $server_path $id
vfs assert_read_only (^touch ($path | path join 'new.txt') | complete) 'creating a file'
vfs assert_read_only (^sh -c $'echo x > "($path | path join 'file.txt')"' | complete) 'writing to a file'
