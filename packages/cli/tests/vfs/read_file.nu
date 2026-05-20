use ../../test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# Reading a file through the mounted VFS returns its contents.

let server_path = mktemp --directory
let server = spawn --directory $server_path --config { vfs: true }
vfs assert_mounted $server_path

let id = tg build (artifact {
	tangram.ts: '
		export default () => tg.directory({ "file.txt": tg.file("contents") })
	'
})

let path = vfs root $server_path $id | path join 'file.txt'
assert ((open --raw $path) == 'contents') 'unexpected file contents'
