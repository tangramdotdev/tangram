use ../../test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# Listing a directory through the mounted VFS returns its entries, including nested ones.

let server_path = mktemp --directory
let server = spawn --directory $server_path --config { vfs: true }
vfs assert_mounted $server_path

let id = tg build (artifact {
	tangram.ts: '
		export default () => tg.directory({
			"a.txt": tg.file("a"),
			"b.txt": tg.file("b"),
			"sub": tg.directory({ "c.txt": tg.file("c") }),
		})
	'
})

let path = vfs root $server_path $id
assert ((ls $path | get name | path basename | sort) == ['a.txt', 'b.txt', 'sub']) 'unexpected entries'
assert ((ls ($path | path join 'sub') | get name | path basename) == ['c.txt']) 'unexpected nested entries'
