use ../lib/test.nu *

# An exact remote get can be served from the principal-scoped remote cache.

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
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

let pid = open ($remote.directory | path join lock) | into int
kill --signal 2 $pid
wait_for_exit $pid "the remote should stop"

let output = with-env { TANGRAM_QUIET: "false" } { tg --url $local.url get --cached foo | complete }
success $output
let cached = $output.stdout | from json
assert equal $cached.id $remote_group.id
assert (($cached | get --optional location) == null) "cached get should not print a location to stdout"
assert (($cached | get --optional tokens) == null) "cached get should not print tokens to stdout"
assert equal ($output.stderr | lines | length) 1 "cached get should print only the resolved referent as info"
assert ($output.stderr | str contains "location=remote") "the cached referent should include its location"
assert ($output.stderr | str contains "tokens") "the cached referent should include its tokens"
