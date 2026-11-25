use ../../util/test.nu *

# With a config that declares a manager that is not running, the CLI manages the server as if the config declared no manager: the hint is to run `tangram server start`, `--start` starts a server that reports the CLI as its manager, and `tangram server stop` stops it.

let manager = manager_not_running
let directory = mktemp -d
let config_path = $directory | path join 'config.json'
{ manager: $manager, vfs: false } | to json | save $config_path
let tangram = [--config $config_path --directory ($directory | path join 'server') --no-remotes]

let output = tangram ...$tangram health | complete
failure $output 'the command should fail without a server'
assert ($output.stderr | str contains 'no server is running') $output.stderr
assert ($output.stderr | str contains '`tangram server start`') $output.stderr

let output = tangram ...$tangram server status | complete
success $output
assert equal ($output.stdout | from json) { status: stopped }

let output = tangram ...$tangram --start health | complete
success $output 'the command should start the server'

let output = tangram ...$tangram server status | complete
success $output
let status = $output.stdout | from json
assert equal $status.status started
assert equal $status.manager cli

let output = tangram ...$tangram server restart | complete
success $output 'the server should restart'

let output = tangram ...$tangram server stop | complete
success $output 'the server should stop'

let output = tangram ...$tangram server status | complete
success $output
assert equal ($output.stdout | from json) { status: stopped }
