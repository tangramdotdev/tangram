use ../../test.nu *

# Expired cached tokens are refreshed when possible and omitted from cached-only responses.

let remote = spawn --cloud --name remote --config {
	sync: { grant_time_to_live: 1 }
}
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

tg --url $remote.url group create foo

let first = tg --url $local.url get foo | from json
assert (($first | get --optional tokens.local) != null) "remote get should return a token"

sleep 2sec

let refreshed = tg --url $local.url get foo | from json
assert (($refreshed | get --optional tokens.local) != null) "remote get should refresh an expired token"
assert ($refreshed.tokens.local != $first.tokens.local) "the refreshed token should be new"

sleep 2sec

let pid = open ($remote.directory | path join lock) | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } "the remote should stop"

let cached = tg --url $local.url get --cached foo | from json
assert (($cached | get --optional tokens.local) == null) "cached get should omit an expired token"
