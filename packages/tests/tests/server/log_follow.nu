use ../../util/test.nu *

# `tangram server log -f` prints the last lines of the log and then what the server writes, moving to each new file as the log rotates.

let directory = mktemp -d
let config_path = $directory | path join 'config.json'
{
	log: { max_files: 100, max_size: 1024 },
	tracing: { filter: 'tangram_http=trace' },
	vfs: false,
} | to json | save $config_path
let server_directory = $directory | path join 'server'
let tangram = [--config $config_path --directory $server_directory --no-remotes]

let output = tangram ...$tangram server start | complete
success $output 'the server should start'
tangram ...$tangram tag get marker-start | complete | ignore

# Follow the log, signal once the follower has printed the request before it started, and then read until the last request.
let fifos = mktemp --directory
let output_fifo = $fifos | path join 'output'
let ready = $fifos | path join 'ready'
^mkfifo $output_fifo $ready
let script = 'output="$1"; shift; tangram "$@" server log -f > "$output" & pid=$!; { grep -m1 -F /tags/marker-start > /dev/null; echo ready > "$READY"; tee "$SEEN" | grep -m1 -F /tags/marker-20 > /dev/null; } < "$output"; status=$?; kill $pid; wait $pid 2> /dev/null; exit $status'
let seen = $directory | path join 'seen'
let follower = job spawn {
	let job_id = job id
	let output = with-env { READY: $ready, SEEN: $seen } {
		^timeout 30 bash -c $script _ $output_fifo ...$tangram | complete
	}
	$output | job send --tag $job_id 0
}
open --raw $ready | ignore

# Make requests that rotate the log several times.
for index in 1..20 {
	tangram ...$tangram tag get $'marker-($index)' | complete | ignore
}

# The follower printed every request in order.
let output = job recv --tag $follower --timeout 30sec
success $output 'the follower should print the last request'
let markers = open --raw $seen | parse --regex '/tags/marker-(\d+)"' | get capture0 | into int | uniq
assert equal $markers (1..20 | each { |index| $index })

let output = tangram ...$tangram server stop | complete
success $output 'the server should stop'
let files = ls ($server_directory | path join 'log') | length
assert ($files >= 2) 'the log should have rotated'
