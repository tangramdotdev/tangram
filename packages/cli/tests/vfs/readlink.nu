use ../../test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# Reading a symlink through the mounted VFS returns its target and resolves to the target's contents.

let server_path = mktemp --directory
let server = spawn --directory $server_path --config { vfs: true }
vfs assert_mounted $server_path

let id = tg build (artifact {
	tangram.ts: '
		export default () => tg.directory({
			"file.txt": tg.file("contents"),
			"link": tg.symlink("file.txt"),
		})
	'
})

let path = vfs root $server_path $id
assert ((^readlink ($path | path join 'link') | str trim) == 'file.txt') 'unexpected link target'
assert ((open --raw ($path | path join 'link')) == 'contents') 'unexpected contents through the link'
