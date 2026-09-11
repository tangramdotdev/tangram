use ../../test.nu *
use ../lib/vfs.nu

# A dependency reference read through the VFS includes a token so a later checkin does not need an authorization graph search.

vfs skip_unless_supported

let server_path = mktemp --directory
let server = server spawn --directory $server_path --config { vfs: true }
vfs assert_mounted $server_path

let module = artifact {
	tangram.ts: '
		export default async () => {
			const dependency = await tg.file("dependency");
			const file = await tg.file({
				contents: "input",
				dependencies: { dependency },
			});
			await file.store();
			return { dependency: dependency.id, file: file.id };
		}
	'
}
let artifacts = tg build $module | from json

let path = vfs root $server_path $artifacts.file
let dependencies = xattr_read 'user.tangram.dependencies' $path
assert equal ($dependencies | normalize) '["dependency?tokens[local]=<token>"]'
let file_token = xattr_read 'user.tangram.token' $path
assert (not ($file_token | is-empty)) 'missing file token xattr'

# The in-server VFS provider issues a permanent, exact token for the dependency.
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
	let body = (
		$token
		| split row '.'
		| get 1
		| decode base64
		| decode utf-8
		| from json
	)
	assert equal $body.expires_at 9223372036854775807
	assert equal $body.resource $artifacts.dependency
	let file_body = (
		$file_token
		| split row '.'
		| get 1
		| decode base64
		| decode utf-8
		| from json
	)
	assert equal $file_body.expires_at 9223372036854775807
	assert equal $file_body.resource $artifacts.file
}
