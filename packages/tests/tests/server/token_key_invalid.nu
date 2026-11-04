use ../../util/test.nu *

# A damaged persisted token signing secret prevents startup instead of silently invalidating existing tokens.

# Use the server's default key files rather than the instance's secret.
let tokens = { secret: { algorithm: blake3, id: 0, path: null } }
for kind in [authentication authorization] {
	let local = server spawn --config { authentication: { tokens: $tokens }, authorization: { tokens: $tokens }, vfs: false }
	server stop $local
	let path = $local.directory | path join $'($kind).key'
	'invalid' | save -f $path

	let output = tangram -c $local.config_path -d $local.directory serve | complete
	failure $output 'the server must reject a damaged signing secret'
	snapshot $output.stderr '
		error an error occurred
		-> failed to start the server
		-> invalid secret

	'
}
