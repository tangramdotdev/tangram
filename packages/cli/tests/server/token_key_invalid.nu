use ../../test.nu *

# A damaged persisted token key prevents startup instead of silently invalidating existing tokens.

for kind in [authentication authorization] {
	let server = server spawn --config { vfs: false }
	server stop $server
	let path = $server.directory | path join $'($kind).key'
	'invalid' | save -f $path

	let output = tangram -c $server.config_path -d $server.directory serve | complete
	failure $output 'the server must reject a damaged private key'
	snapshot $output.stderr '
		error an error occurred
		-> failed to start the server
		-> invalid private key

	'
	assert equal (open --raw $path) 'invalid'
}
