use ../../util/test.nu *

# Stopping with force a server that systemd manages is refused like a stop without force, and the server is not signaled.

let directory = mktemp -d
let config_path = $directory | path join 'config.json'
{ server: { kill_timeout: 0.1, stop_timeout: 0.1 }, vfs: false } | to json | save $config_path
let server_directory = $directory | path join 'server'
let tangram = [--config $config_path --directory $server_directory --no-remotes]

let server = stubborn_server $server_directory systemd

let output = tangram ...$tangram server stop --force | complete
failure $output 'stopping with force a server that systemd manages should fail'
assert ($output.stderr | str contains 'the server is managed by systemd') $output.stderr

# The server is still running, so kill it with the stop that a manager of the server would send.
stubborn_server ping $server
let pid = server_pid $server_directory
kill --signal 9 $pid
job recv --timeout 10sec | ignore
