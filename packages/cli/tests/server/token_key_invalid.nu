use ../lib/test.nu *

# A damaged persisted token key prevents startup instead of silently invalidating existing tokens.

for kind in [authentication authorization] {
	let local = server spawn --config { vfs: false }
	server stop $local
	let path = $local.directory | path join $'($kind).key'
	'invalid' | save -f $path

	let output = tangram -c $local.config_path -d $local.directory serve | complete
	failure $output 'the server must reject a damaged private key'
	snapshot $output.stderr '
		error an error occurred
		-> failed to start the server
		-> invalid private key

	'
	assert equal (open --raw $path) 'invalid'
}

# Validate the private key independently of the configured public keys.
let local = server spawn --config {
	verification: {
		tokens: { public_keys: [] }
	}
	vfs: false
}
server stop $local
let path = $local.directory | path join 'authorization.key'
'invalid' | save -f $path

let output = tangram -c $local.config_path -d $local.directory serve | complete
failure $output 'the server must reject a damaged private key'
snapshot $output.stderr '
	error an error occurred
	-> failed to start the server
	-> invalid private key

'
assert equal (open --raw $path) 'invalid'
