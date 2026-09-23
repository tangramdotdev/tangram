use ../lib/test.nu *

# Signaling a sandboxed process also kills a child that inherited its process group and log pipe.

let server = server spawn
let mount = artifact {}
let output = tg spawn --sandbox --mount $"($mount):/target" --verbose --executable /bin/sh -- -c '/bin/sh -c "echo ready; while :; do :; done"; :' | from json
let process = $output.process
let sandbox = tg process get $process | from json | get sandbox

wait_until {
	(tg log $process | complete).stdout | str contains 'ready'
} 'the child should produce a log before the process is signaled'

success (tg signal --signal KILL $process | complete)
let output = timeout 10s tg wait $process | from json
assert equal $output.exit 137 'the process should exit after its group is killed'
success (timeout 10s tg wait $sandbox | complete) 'the sandbox should finish after the child is killed'
