use ../../util/test.nu *

# A lock file that a killed server left behind is stale, because no process holds its lock. `tangram server status` reports the server as stopped, `tangram server stop` succeeds without signaling the process that now has the lock file's pid, and `tangram server start` starts a new server.

let directory = mktemp -d
let config_path = $directory | path join 'config.json'
{ vfs: false } | to json | save $config_path
let server_directory = $directory | path join 'server'
let tangram = [--config $config_path --directory $server_directory --no-remotes]

# Start a server, then kill it so that it leaves its lock file behind.
let output = tangram ...$tangram server start | complete
success $output 'the server should start'
let pid = server_pid $server_directory
kill --signal 9 $pid
wait_for_exit $pid 'the server must exit'

# Spawn an unrelated process that reports whether it received a signal when the test releases it, and write its pid to the lock file in place of the killed server's.
let ready = $directory | path join 'ready'
let release = $directory | path join 'release'
^mkfifo $ready $release
let script = '
import os, signal, sys
signaled = []
signal.signal(signal.SIGINT, lambda number, frame: signaled.append(number))
signal.signal(signal.SIGTERM, lambda number, frame: signaled.append(number))
with open(sys.argv[1], "w") as ready:
	ready.write(str(os.getpid()))
with open(sys.argv[2]) as release:
	release.read()
print("signaled" if signaled else "not signaled")
'
let process = job spawn {
	^python3 -c $script $ready $release | complete | job send 0
}
let unrelated = open --raw $ready | into int
{ manager: cli, pid: $unrelated } | to json --raw | save --force ($server_directory | path join 'lock')

# The status reports that the server is stopped.
let output = tangram ...$tangram server status | complete
success $output
assert equal ($output.stdout | from json) { status: stopped }

# Stopping succeeds without signaling the unrelated process.
let output = tangram ...$tangram server stop | complete
success $output 'stopping with a stale lock file should succeed'
'' | save --append $release
let output = job recv --timeout 10sec
success $output 'the unrelated process should exit after it is released'
assert equal ($output.stdout | str trim) 'not signaled' 'the unrelated process must not be signaled'

# A new server starts.
let output = tangram ...$tangram server start | complete
success $output 'the server should start after a stale lock file'
let output = tangram ...$tangram server status | complete
success $output
let status = $output.stdout | from json
assert equal $status.status started
assert ($status.pid != $unrelated) 'the status must report the new server'

let output = tangram ...$tangram server stop | complete
success $output 'the new server should stop'
