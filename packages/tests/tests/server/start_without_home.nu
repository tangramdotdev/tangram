use ../../util/test.nu *

# Starting a server succeeds when the home directory does not exist or the home variable is unset, and relative paths resolve against the caller's working directory.

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

# Start a server with relative config, directory, and socket paths.
do {
	cd $directory
	let tangram = [--config config.json --directory relative --no-remotes --url 'http+unix://relative.sock']
	let output = tangram ...$tangram server start | complete
	success $output 'the server should start with relative paths'
	let output = tangram ...$tangram server stop | complete
	success $output 'the server should stop'
}
