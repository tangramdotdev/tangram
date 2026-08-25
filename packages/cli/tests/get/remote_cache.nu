use ../../test.nu *

# An exact remote get can be served from the principal-scoped remote cache.

let remote = server spawn --cloud --name remote --tokens
let local = server spawn --name local --tokens --config {
	remotes: { default: { url: $remote.url } }
}

let remote_group = tg --url $remote.url group create foo | from json

let output = with-env { TANGRAM_QUIET: "false" } { tg --url $local.url get foo | complete }
success $output
let group = $output.stdout | from json
assert equal $group.id $remote_group.id
assert (($group | get --optional location) == null) "remote get should not print a location to stdout"
assert (($group | get --optional tokens) == null) "remote get should not print tokens to stdout"
assert equal ($output.stderr | lines | length) 1 "remote get should print only the resolved referent as info"
assert ($output.stderr | str contains "location=remote") "the resolved referent should include its location"
assert ($output.stderr | str contains "tokens") "the resolved referent should include its tokens"

let requests = (
	open ($local.directory | path join database)
	| query db 'select request from remote_cache order by request'
	| get request
	| each { from json }
)
let request_kinds = $requests | get kind
assert (
	$request_kinds | all { $in in [get group_get] }
) "an exact get should not use list or match"

let pid = open ($remote.directory | path join lock) | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } "the remote should stop"

let output = with-env { TANGRAM_QUIET: "false" } { tg --url $local.url get --cached foo | complete }
success $output
let cached = $output.stdout | from json
assert equal $cached.id $remote_group.id
assert (($cached | get --optional location) == null) "cached get should not print a location to stdout"
assert (($cached | get --optional tokens) == null) "cached get should not print tokens to stdout"
assert equal ($output.stderr | lines | length) 1 "cached get should print only the resolved referent as info"
assert ($output.stderr | str contains "location=remote") "the cached referent should include its location"
assert ($output.stderr | str contains "tokens") "the cached referent should include its tokens"
