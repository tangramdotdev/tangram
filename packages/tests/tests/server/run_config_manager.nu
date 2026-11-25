use ../../util/test.nu *

# A server run with a config that declares a manager reports that manager, and `--manager` overrides it, even with a manager that is not running.

let local = server spawn
server stop $local
open $local.config_path | upsert manager app | save -f $local.config_path

# Run the server with the manager from the config.
let supervisor = mktemp --directory
track_supervisor $supervisor
^tangram_tests_supervisor start --description server --directory $supervisor --name local --notify -- tangram -c $local.config_path -d $local.directory -u $local.url serve | from json | ignore

let output = tg -d $local.directory server status | complete
success $output
assert equal ($output.stdout | from json | get manager) app
server stop { supervisor: $supervisor }

# Run the server with a manager flag that overrides the config, which the server records even though that manager is not running.
let manager = manager_not_running
let supervisor = mktemp --directory
track_supervisor $supervisor
^tangram_tests_supervisor start --description server --directory $supervisor --name local --notify -- tangram -c $local.config_path -d $local.directory -u $local.url serve --manager $manager | from json | ignore

let output = tg -d $local.directory server status | complete
success $output
assert equal ($output.stdout | from json | get manager) $manager
server stop { supervisor: $supervisor }
