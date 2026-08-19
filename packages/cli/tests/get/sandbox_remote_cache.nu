use ../../test.nu *

# An exact sandbox get can be served from the principal-scoped remote cache.

let root_token = random chars
let remote = spawn --cloud --name remote --preserve-keys --config {
	authentication: { root: { token: $root_token } },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [indexer runner],
	runner: { id: $created.runner.id, remote: "default", token: $created.token.token },
}
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let sandbox = tg --url $remote.url sandbox create --no-network | str trim

let output = tg --url $local.url get $sandbox | from json
assert equal $output.id $sandbox
assert equal $output.location remote

let requests = (
	open ($local.directory | path join database)
	| query db 'select request from remote_cache order by request'
	| get request
	| each { from json }
)
assert ($requests | any {|request| $request.kind == sandbox_get })

tg --url $remote.url sandbox destroy $sandbox
let pid = open ($remote.directory | path join lock) | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } "the remote should stop"

let cached = tg --url $local.url get --cached $sandbox | from json
assert equal $cached.id $sandbox
assert equal $cached.location remote
