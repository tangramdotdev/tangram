use ../../util/test.nu *

# A server run with a config that declares a manager reports that manager, and `--manager` overrides it.

let local = server spawn
server stop $local
open $local.config_path | upsert manager systemd | save -f $local.config_path

# Run the server with the manager from the config.
let supervisor = mktemp --directory
track_supervisor $supervisor
^tangram_tests_supervisor start --description server --directory $supervisor --name local --notify -- tangram -c $local.config_path -d $local.directory -u $local.url serve | from json | ignore

let output = tg -d $local.directory server status | complete
success $output
assert equal ($output.stdout | from json | get manager) systemd
server stop { supervisor: $supervisor }

# Run the server with a manager flag that overrides the config.
let supervisor = mktemp --directory
track_supervisor $supervisor
^tangram_tests_supervisor start --description server --directory $supervisor --name local --notify -- tangram -c $local.config_path -d $local.directory -u $local.url serve --manager openrc | from json | ignore

let output = tg -d $local.directory server status | complete
success $output
assert equal ($output.stdout | from json | get manager) openrc
server stop { supervisor: $supervisor }
