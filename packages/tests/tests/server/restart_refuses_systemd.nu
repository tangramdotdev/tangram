use ../../util/test.nu *

# Restarting a server that systemd manages fails with a hint to use systemctl, and leaves the server running.

let local = server spawn
server stop $local

# Start the server under a supervisor with systemd as its manager, which waits for it to signal readiness.
let supervisor = mktemp --directory
track_supervisor $supervisor
^tangram_tests_supervisor start --description server --directory $supervisor --name local --notify -- tangram -c $local.config_path -d $local.directory -u $local.url serve --manager systemd | from json | ignore

let output = tg -d $local.directory server restart | complete
failure $output 'restarting a server that systemd manages should fail'
assert ($output.stderr | str contains 'the server is managed by systemd') $output.stderr
assert ($output.stderr | str contains '`systemctl restart tangram`') $output.stderr

let output = tg --url $local.url health | complete
success $output 'the server should still be running'

server stop { supervisor: $supervisor }
