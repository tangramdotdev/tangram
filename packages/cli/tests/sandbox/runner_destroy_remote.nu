use ../../test.nu *

# A remote sandbox's local index entry must not send destruction through local control.

let root_token = random chars
let remote = server spawn --name remote --config {
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	authentication: { root: { token: $root_token } },
	remotes: { default: { token: $root_token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let sandbox = tg --url $remote.url --token $root_token sandbox create | str trim
let socket = $runner.url | str replace 'http+unix://' '' | url decode
let headers = { Authorization: $'Bearer ($root_token)', 'Content-Type': 'application/json' }
tg --url $runner.url --token $root_token index
let output = http get --max-time 10sec --unix-socket $socket --headers $headers $'http://localhost/sandboxes/($sandbox)?location=remote'
assert equal $output.location remote
assert equal $output.data.status started

let output = http post --full --allow-errors --max-time 10sec --unix-socket $socket --headers $headers $'http://localhost/sandboxes/($sandbox)/destroy' '{"location":"local"}'
assert equal $output.status 404

let output = http post --full --allow-errors --max-time 10sec --unix-socket $socket --headers $headers $'http://localhost/sandboxes/($sandbox)/destroy' '{}'
assert equal $output.status 200 "destruction without a location must reach the owning remote"
timeout 30s tg --url $remote.url --token $root_token sandbox wait $sandbox | ignore
tg --url $runner.url --token $root_token index

# Destroyed data must preserve the remote location as well.
let output = http post --full --allow-errors --max-time 10sec --unix-socket $socket --headers $headers $'http://localhost/sandboxes/($sandbox)/destroy' '{"location":"local"}'
assert equal $output.status 404
let output = http post --full --allow-errors --max-time 10sec --unix-socket $socket --headers $headers $'http://localhost/sandboxes/($sandbox)/destroy' '{}'
assert equal $output.status 409
