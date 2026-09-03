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
			const file = tg.file({
				contents: "input",
				dependencies: { dependency },
			});
			return { dependency: dependency.id, file: file.id };
		}
	'
}
let artifacts = tg build $module | from json

let path = vfs root $server_path $artifacts.file
let dependencies = xattr_read 'user.tangram.dependencies' $path
assert equal ($dependencies | normalize) '["dependency?tokens[local]=<token>"]'

# The in-server VFS provider issues an exact token for the dependency.
if $nu.os-info.name == 'linux' {
	let reference = $dependencies | from json | first
	let token = (
		$"http://localhost/($reference)"
		| url parse
		| get params
		| where key == 'tokens[local]'
		| first
		| get value
	)
	let resource = (
		$token
		| split row '.'
		| get 1
		| decode base64
		| decode utf-8
		| from json
		| get resource
	)
	assert equal $resource $artifacts.dependency
}
