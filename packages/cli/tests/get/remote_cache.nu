use ../../test.nu *

# An exact remote get can be served from the principal-scoped remote cache.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let remote_group = tg --url $remote.url group create foo | from json

let group = tg --url $local.url get foo | from json
assert equal $group.id $remote_group.id
assert equal $group.location remote
assert (($group | get --optional token) != null) "remote get should return a token"

let pid = open ($remote.directory | path join lock) | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } "the remote should stop"

let cached = tg --url $local.url get --cached foo | from json
assert equal $cached.id $remote_group.id
assert equal $cached.location remote
assert (($cached | get --optional token) != null) "cached get should preserve the token"
