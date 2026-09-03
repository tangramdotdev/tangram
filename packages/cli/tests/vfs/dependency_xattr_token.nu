use ../../test.nu *
use ../lib/vfs.nu

# A dependency reference read through the VFS includes a token so a later checkin does not need an authorization graph search.

vfs skip_unless_supported

let server_path = mktemp --directory
let server = server spawn --directory $server_path --config { vfs: true }
vfs assert_mounted $server_path

let module = artifact {
	tangram.ts: '
		export default () => {
			const dependency = tg.file("dependency");
			return tg.file({
				contents: "input",
				dependencies: { dependency },
			});
		}
	'
}
let id = tg build $module | str trim

let path = vfs root $server_path $id
let dependencies = xattr_read 'user.tangram.dependencies' $path | normalize
assert equal $dependencies '["dependency?tokens[local]=<token>"]'
