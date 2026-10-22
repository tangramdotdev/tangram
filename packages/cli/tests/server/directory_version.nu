use ../lib/test.nu *

# A server restarts on its initialized directory and refuses to start when the directory version is unsupported or missing.

let local = server spawn --config { vfs: false }
let version_path = $local.directory | path join 'version'
let server = server restart $local
success (tg health | complete) 'the initialized directory must remain usable'
server stop $server

for version in ['invalid' '1' '2'] {
	$version | save -f $version_path
	let output = tangram -c $local.config_path -d $local.directory serve | complete
	failure $output 'the server must reject an unsupported directory version'
}

# An incomplete initialization is not recovered.
if ($local.config.database?.kind? | default 'sqlite') in ['sqlite' 'turso'] {
	rm $version_path
	let output = tangram -c $local.config_path -d $local.directory serve | complete
	failure $output 'initializing an existing schema without a directory version must fail'
	assert ($output.stderr | str contains 'failed to create the database schema')
}
