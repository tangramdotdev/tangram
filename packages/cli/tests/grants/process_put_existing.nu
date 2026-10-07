use ../lib/test.nu *

# PUT with matching process data and children grants node permission. PUT with a different exit code fails.
let root_token = random chars
let local = server spawn --config { authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let process = "pcs_00081061050r3gg28a1c60t3gf20"
let data = {
	children: [],
	command: "cmd_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0",
	created_at: 0,
	exit: 0,
	finished_at: 1,
	host: "x86_64-linux",
	status: "finished",
}
tg --token $alice.token process put $process ($data | to json)
success (tg --token $alice.token process get $process | complete)
failure (tg --token $bob.token process put $process ($data | upsert exit 1 | to json) | complete)
failure (tg --token $bob.token process get $process | complete)
tg --token $bob.token process put $process ($data | reject children | to json)
failure (tg --token $bob.token process get $process | complete)
tg --token $bob.token process put $process ($data | to json)
success (tg --token $bob.token process get $process | complete)

# Check the returned proof tokens as well as persisted permissions.
let socket = $local.url | str replace 'http+unix://' '' | url decode
let response = http put --raw --unix-socket $socket --headers { Authorization: $'Bearer ($bob.token)' } --content-type application/json $'http://localhost/processes/($process)' { data: $data } | from json
assert (($response | get --optional tokens.local | default [] | length) > 0)
for token in [$alice.token $bob.token] {
	failure (tg --token $token process put $process ($data | upsert exit 1 | to json) | complete)
}
let stored = tg --token $alice.token process get $process | from json
assert equal $stored.exit 0

# Root cannot modify a finished process.
failure (tg --token $root_token process put $process ($data | upsert exit 1 | to json) | complete)
let stored = tg --token $alice.token process get $process | from json
assert equal $stored.exit 0
