use ../lib/test.nu *

# DestroySandbox control requests enforce runner ownership and are idempotent.

const driver = path self ../lib/runner_control_destroy_sandbox.py
let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 300 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let sandbox = tg --url $remote.url --token $root_token sandbox create | str trim
let socket = $remote.url | str replace 'http+unix://' '' | url decode
let other = tg --url $remote.url --token $root_token runner create | from json
$other | to json | save other.json
python3 $driver $socket other.json $sandbox false
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) started

let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_until { ps | where pid == $pid | is-empty } "the runner must exit"
$created | to json | save runner.json
python3 $driver $socket runner.json $sandbox true
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) destroyed
