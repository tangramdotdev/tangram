use ../../test.nu *

# An exact sandbox get can be served from the principal-scoped remote cache.

let remote = spawn --cloud --name remote
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
