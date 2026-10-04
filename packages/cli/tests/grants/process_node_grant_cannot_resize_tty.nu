use ../lib/test.nu *

# Granting process_node to a principal should not allow that principal to resize the process's tty.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

let path = artifact { tangram.ts: 'export default async function () { await tg.sleep(30); return "done"; }' }
let process = tg --token $alice.token spawn --no-tokens --network=true --tty=24,80 $path | referent node

# Alice grants Eve read-only access to the process.
tg --token $alice.token grant $eve.user.id process_node $process
tg --token $alice.token index

# A process_node grant must not confer the ability to resize the process's tty.
let socket = $local.url | str replace 'http+unix://' '' | url decode
let response = (
	http put
		--allow-errors
		--full
		--max-time 10sec
		--unix-socket $socket
		--content-type application/json
		--headers { Authorization: $'Bearer ($eve.token)' }
		$'http://localhost/processes/($process)/tty/size'
		{ size: { rows: 40, cols: 100 } }
)
assert equal $response.status 404 "a process_node grant must not confer the ability to resize the process's tty."
