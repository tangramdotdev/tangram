use ../lib/test.nu *

# Only destroyed sandbox get responses can be served from the principal-scoped remote cache.

let root_token = random chars
let remote = server spawn --cloud --name remote --preserve-keys --config {
	authentication: { root: { token: $root_token } },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let sandbox = tg --url $remote.url sandbox create --no-network | str trim

let output = tg --url $local.url get $sandbox | from json
assert equal $output.data.id $sandbox
assert equal $output.location remote
assert equal $output.data.status started

let requests = (
	open ($local.directory | path join database.sqlite3)
	| query db 'select request from remote_cache order by request'
	| get request
	| each { from json }
)
assert (not ($requests | any {|request| $request.kind == sandbox_get }))
failure (tg --url $local.url sandbox get --cached $sandbox | complete)

tg --url $remote.url sandbox destroy $sandbox
timeout 10s tg --url $remote.url sandbox wait --source=index $sandbox | ignore
let output = tg --url $local.url get $sandbox | from json
assert equal $output.data.status destroyed

let requests = (
	open ($local.directory | path join database.sqlite3)
	| query db 'select request from remote_cache order by request'
	| get request
	| each { from json }
)
assert ($requests | any {|request| $request.kind == sandbox_get })

let pid = open ($remote.directory | path join lock) | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } "the remote should stop"

let cached = tg --url $local.url get --cached $sandbox | from json
assert equal $cached.data.id $sandbox
assert equal $cached.location remote
assert equal $cached.data.status destroyed
