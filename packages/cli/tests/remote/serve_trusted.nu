use ../lib/test.nu *

# The serve command can mark one of its remotes as trusted.

let remote_upstream = server spawn --name remote-upstream
let local = server spawn --name local
server stop $local

let config_path = $local.config_path
let directory = $local.directory
let url = $local.url
let remote = $'default=($remote_upstream.url)'

# Start the server under a supervisor, which waits for it to signal readiness.
let supervisor = mktemp --directory
track_supervisor $supervisor
^tangram_cli_test_supervise start --description server --directory $supervisor --name local --ready-fd 3 -- tangram -c $config_path -d $directory -u $url --remotes $remote serve --trusted-remotes default --ready-fd 3 | from json | ignore

let remote = tg --url $url remote get default | from json
assert equal $remote.trusted true "the serve option should mark the remote as trusted"

server stop { supervisor: $supervisor }
