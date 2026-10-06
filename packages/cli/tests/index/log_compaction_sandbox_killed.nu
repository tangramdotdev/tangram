use ../lib/test.nu *

# Killing a sandbox while its process is logging must not block indexing.

let local = server spawn
let output = tg spawn --sandbox --verbose --executable /bin/sh -- -c 'echo ready; while :; do :; done' | from json
let process = $output.process

wait_until {
	(tg log $process | complete).stdout | str contains 'ready'
} 'the process should produce a log before its sandbox is killed'

let pids = ps --long | where { |row| ($row.command | str contains 'sandbox serve') and ($row.command | str contains $local.directory) } | get pid
assert (not ($pids | is-empty)) 'the sandbox should be running'
for pid in $pids {
	kill --signal 9 $pid
}
success (timeout 30s tg wait $process | complete) 'the process should finish after its sandbox is killed'

let output = timeout 30s tg index | complete
success $output 'the index wait should not block on the log compaction'

let output = timeout 10s tg log --no-timeout $process | complete
success $output 'the compacted log should be readable after its sandbox is killed'
assert ($output.stdout | str contains 'ready') 'the compacted log should preserve the received output'
