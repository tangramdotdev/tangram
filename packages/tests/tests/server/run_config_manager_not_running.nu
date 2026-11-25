use ../../util/test.nu *

# A server run with a config that declares a manager that is not running reports no manager, so it can only be stopped by stopping its process.

let local = server spawn
server stop $local
let manager = manager_not_running
open $local.config_path | upsert manager $manager | save -f $local.config_path

let supervisor = mktemp --directory
track_supervisor $supervisor
^tangram_tests_supervisor start --description server --directory $supervisor --name local --notify -- tangram -c $local.config_path -d $local.directory -u $local.url serve | from json | ignore

let output = tg -d $local.directory server status | complete
success $output
let status = $output.stdout | from json
assert equal $status.status started
assert equal ($status | get --optional manager) null

let output = tg -d $local.directory server stop | complete
failure $output 'stopping a server without a manager should fail'
assert ($output.stderr | str contains 'the server was not started by `tangram server start`') $output.stderr

server stop { supervisor: $supervisor }
