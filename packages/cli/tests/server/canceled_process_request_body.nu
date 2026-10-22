use ../lib/test.nu *

# Canceling a process while its client is sending a request body does not log a server error. A cache hit cancels its candidate process this way.

let local = server spawn --config {
	advanced: { checkpoints: true }
	tracing: { stderr_format: 'json' }
}

# Start a process whose write request never finishes its body.
let write = checkpoint watch write.request
let process = tg spawn --sandbox --verbose --executable /bin/sh -- -c '{ printf partial; while :; do :; done; } | tg write' | from json

# Cancel the process while the server is reading the request body.
checkpoint pass $write | ignore
checkpoint unwatch $write
tg cancel $process.process $process.lease
let output = tg wait $process.process | from json
assert equal $output.exit 1 'the process should be canceled'

let errors = server_errors $local
snapshot $errors ''
