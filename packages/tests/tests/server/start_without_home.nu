use ../../util/test.nu *

# Starting a server succeeds when the home directory does not exist.

let directory = mktemp --directory
let config_path = $directory | path join 'config.json'
{ vfs: false } | to json | save $config_path
let tangram = [--config $config_path --directory ($directory | path join 'server') --no-remotes]
with-env { HOME: ($directory | path join 'missing') } {
	let output = tangram ...$tangram server start | complete
	success $output 'the server should start without the home directory'
	let output = tangram ...$tangram server stop | complete
	success $output 'the server should stop'
}
