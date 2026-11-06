use ../../util/test.nu *

# Starting a server succeeds when the home directory does not exist or is unset, as long as the directory is known, and fails with a clear error otherwise.

let directory = mktemp --directory
let config_path = $directory | path join 'config.json'
{ vfs: false } | to json | save $config_path
let tangram = [--config $config_path --directory ($directory | path join 'server') --no-remotes]

# Start a server when the home directory does not exist.
with-env { HOME: ($directory | path join 'missing') } {
	let output = tangram ...$tangram server start | complete
	success $output 'the server should start without the home directory'
	let output = tangram ...$tangram server stop | complete
	success $output 'the server should stop'
}

# Start a server when the home variable is unset.
do {
	hide-env HOME
	let output = tangram ...$tangram server start | complete
	success $output 'the server should start with the home variable unset'
	let output = tangram ...$tangram server stop | complete
	success $output 'the server should stop'
}

# Fail with a clear error when the home variable is unset and the directory is not specified.
do {
	hide-env HOME
	let output = tangram --no-remotes server start | complete
	assert ($output.exit_code != 0) 'the server should not start without a directory'
	assert ($output.stderr | str contains 'failed to get the directory') 'the error should explain the failure'
}

# Start a server with relative config and directory paths.
do {
	cd $directory
	let tangram = [--config config.json --directory relative --no-remotes]
	let output = tangram ...$tangram server start | complete
	success $output 'the server should start with relative paths'
	let output = tangram ...$tangram server stop | complete
	success $output 'the server should stop'
}
