use ../lib/test.nu *

# A destroyed sandbox can be pushed without its processes, then pushed again with its processes.

let root_token = random chars
let remote = server spawn --cloud --name remote --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

let path = artifact {
	tangram.ts: '
		export default function () {
			return tg.file("output")
		}
	',
}
let process = tg build --detach $path | str trim
tg wait --source=index $process
let sandbox = tg get $process | from json | get sandbox
tg wait --source=index $sandbox
tg index

tg push $sandbox
let remote_sandbox = tg --url $remote.url --token $alice.token sandbox get $sandbox | from json
assert equal $remote_sandbox.data.id $sandbox
assert equal $remote_sandbox.data.status destroyed
assert (($remote_sandbox | get --optional tokens.local) != null) "sandbox get should return a token"
failure (tg --url $remote.url --token $alice.token process get $process | complete)

# The sandbox retains its ordered process IDs even before those process records are transferred.
let socket = $remote.url | str replace 'http+unix://' '' | url decode
let output = http get --raw --max-time 10sec --unix-socket $socket --headers { Authorization: $'Bearer ($alice.token)' } $'http://localhost/sandboxes/($sandbox)/processes?source=index'
let processes = $output | lines | where { $in starts-with 'data: ' } | each { str substring 6.. | from json } | get data | flatten
assert equal $processes [$process]

tg push --sandbox-processes $sandbox
success (tg --url $remote.url --token $alice.token process get $process | complete)
