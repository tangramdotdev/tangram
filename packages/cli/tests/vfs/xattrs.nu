use ../../test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# Listing and reading a file's Tangram xattrs through the mounted VFS returns its metadata.

let server_path = mktemp --directory
let server = spawn --directory $server_path --config { vfs: true }
vfs assert_mounted $server_path

let id = tg build (artifact {
	tangram.ts: '
		export default () => {
			let dependency = tg.file("dependency");
			return tg.directory({
				"file.txt": tg.file({
					contents: "contents",
					dependencies: { dependency },
					module: "ts",
				}),
			});
		}
	'
})

let path = vfs root $server_path $id | path join 'file.txt'
let names = xattr_list $path | sort
assert ($names == ['user.tangram.dependencies', 'user.tangram.module']) 'unexpected xattr names'
assert ((xattr_read 'user.tangram.dependencies' $path | from json) == ['dependency']) 'unexpected dependency xattr'
assert ((xattr_read 'user.tangram.module' $path) == 'ts') 'unexpected module xattr'
