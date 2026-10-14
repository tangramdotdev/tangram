use ../lib/test.nu *

# The directory version controls schema initialization and survives restarts.
let local = server spawn --config { vfs: false }
let version_path = $local.directory | path join 'version'
assert equal (open --raw $version_path) '0'
let version_modified = ls $version_path | get modified | first
let server = server restart $local
success (tg health | complete) 'the initialized directory must remain usable'
assert equal (open --raw $version_path) '0'
assert equal (ls $version_path | get modified | first) $version_modified
server stop $server

for version in ['invalid' '1' '2'] {
	$version | save -f $version_path
	let output = tangram -c $local.config_path -d $local.directory serve | complete
	failure $output 'the server must reject an unsupported directory version'
	assert equal (open --raw $version_path) $version
}

# An incomplete initialization is not recovered or marked complete.
if ($local.config.database?.kind? | default 'sqlite') in ['sqlite' 'turso'] {
	rm $version_path
	let output = tangram -c $local.config_path -d $local.directory serve | complete
	failure $output 'initializing an existing schema without a directory version must fail'
	assert ($output.stderr | str contains 'failed to create the database schema')
	assert not ($version_path | path exists)
}
