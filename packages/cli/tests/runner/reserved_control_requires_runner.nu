use ../../test.nu *

# A reserved process or sandbox control connection is refused for a principal that is not a runner.

let root_token = random chars
let remote = server spawn --name remote --config {
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let socket = $remote.url | str replace 'http+unix://' '' | url decode
let headers = { Authorization: $'Bearer ($root_token)', 'Content-Type': 'application/vnd.tangram.process-control' }
let output = http post --full --allow-errors --max-time 10sec --unix-socket $socket --headers $headers 'http://localhost/processes/control?reserved=true' ''
assert equal $output.status 500 "a root principal must not reserve a process control connection"
assert ($output.body | to text | str contains 'requires a runner') ($output.body | to text)

let headers = { Authorization: $'Bearer ($root_token)', 'Content-Type': 'text/event-stream' }
let output = http post --full --allow-errors --max-time 10sec --unix-socket $socket --headers $headers 'http://localhost/sandboxes/control?reserved=true' ''
assert equal $output.status 500 "a root principal must not reserve a sandbox control connection"
assert ($output.body | to text | str contains 'requires a runner') ($output.body | to text)
