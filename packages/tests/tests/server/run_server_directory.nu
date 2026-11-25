use ../../util/test.nu *

# `tangram server run` runs the server in the directory that `server.directory` sets when no `--directory` is passed.

# Remove the inherited URL and token, so that the commands find the server through the config.
hide-env --ignore-errors TANGRAM_URL TANGRAM_TOKEN

let directory = mktemp -d
let config_path = $directory | path join 'config.json'
{ server: { directory: ($directory | path join 'server') }, vfs: false } | to json | save $config_path

let supervisor = mktemp --directory
track_supervisor $supervisor
^tangram_tests_supervisor start --description server --directory $supervisor --name local --notify -- tangram -c $config_path --no-remotes serve | from json | ignore

let output = tangram -c $config_path --no-remotes server status | complete
success $output
let status = $output.stdout | from json
assert equal $status.status started
assert ($status.pid != null) 'the status should report the pid from the lock file in the server directory'

let output = tangram -c $config_path --no-remotes health | complete
success $output 'a client should reach the server through the socket in the server directory'

server stop { supervisor: $supervisor }
