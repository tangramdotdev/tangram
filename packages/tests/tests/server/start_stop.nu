use ../../util/test.nu *

# A server that `tangram server start` started reports the CLI as its manager, and `tangram server stop` stops it.

let directory = mktemp -d
let config_path = $directory | path join 'config.json'
{ vfs: false } | to json | save $config_path
let tangram = [--config $config_path --directory ($directory | path join 'server') --no-remotes]
let output = tangram ...$tangram server start | complete
success $output 'the server should start'

let output = tangram ...$tangram server status | complete
success $output
let status = $output.stdout | from json
assert equal $status.status started
assert equal $status.manager cli

let output = tangram ...$tangram server stop | complete
success $output 'the server should stop'

let output = tangram ...$tangram server status | complete
success $output
assert equal ($output.stdout | from json) { status: stopped }
