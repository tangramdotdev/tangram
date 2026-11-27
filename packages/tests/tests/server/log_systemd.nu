use ../../util/test.nu *

# `tangram server log` for a server that systemd manages fails with a hint to read the journal.

let local = server spawn
server stop $local

# Start the server under a supervisor with systemd as its manager, which waits for it to signal readiness.
let supervisor = mktemp --directory
track_supervisor $supervisor
^tangram_tests_supervisor start --description server --directory $supervisor --name local --notify -- tangram -c $local.config_path -d $local.directory -u $local.url serve --manager systemd | from json | ignore

let output = tg -d $local.directory server log | complete
failure $output 'printing the log should fail for a server that systemd manages'
assert ($output.stderr | str contains 'the server is managed by systemd') $output.stderr
assert ($output.stderr | str contains '`journalctl -u tangram`') $output.stderr

server stop { supervisor: $supervisor }
