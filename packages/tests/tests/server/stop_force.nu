use ../../util/test.nu *

# A server that does not exit after SIGINT and SIGTERM is left running by `tangram server stop`, which fails, and is killed by `tangram server stop --force`, after which `tangram server status` reports it as stopped and `tangram server start` starts a new server.

let directory = mktemp -d
let config_path = $directory | path join 'config.json'
{ server: { kill_timeout: 0.1, stop_timeout: 0.1 }, vfs: false } | to json | save $config_path
let server_directory = $directory | path join 'server'
let tangram = [--config $config_path --directory $server_directory --no-remotes]

let server = stubborn_server $server_directory

# Stopping without force fails and leaves the server running.
let output = tangram ...$tangram server stop | complete
failure $output 'stopping a server that does not exit should fail without force'
assert ($output.stderr | str contains 'failed to terminate the server') $output.stderr
stubborn_server ping $server

# Stopping with force kills the server.
let output = tangram ...$tangram server stop --force | complete
success $output 'stopping a server that does not exit should succeed with force'
let output = job recv --timeout 10sec
assert ($output.exit_code != 0) 'the server must have been killed'

# The status reports that the server is stopped.
let output = tangram ...$tangram server status | complete
success $output
assert equal ($output.stdout | from json) { status: stopped }

# A new server starts.
let output = tangram ...$tangram server start | complete
success $output 'the server should start after a forced stop'
let output = tangram ...$tangram server status | complete
success $output
assert equal ($output.stdout | from json | get status) started

let output = tangram ...$tangram server stop | complete
success $output 'the new server should stop'
