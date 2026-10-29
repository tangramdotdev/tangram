use ../../util/test.nu *

# Expired cached tokens are refreshed when possible and omitted from cached-only responses.

# Give both servers a simulated clock, so that tokens expire without waiting.
let now = '2026-01-01T00:00:00Z'
let remote = server spawn --cloud --name remote --now $now --config {
	sync: { permission_time_to_live: 1 }
}
let local = server spawn --name local --now $now --config {
	remotes: { default: { url: $remote.url } }
}

tg --url $remote.url group create foo

let first_output = with-env { TANGRAM_QUIET: "false" } { tg --url $local.url get foo | complete }
success $first_output
let first = $first_output.stdout | from json
assert (($first | get --optional tokens) == null) "remote get should not print tokens to stdout"
assert ($first_output.stderr | str contains "tokens") "the resolved referent should include a token"

advance_time $remote 2sec
advance_time $local 2sec

let refreshed_output = with-env { TANGRAM_QUIET: "false" } { tg --url $local.url get foo | complete }
success $refreshed_output
let refreshed = $refreshed_output.stdout | from json
assert (($refreshed | get --optional tokens) == null) "remote get should not print tokens to stdout"
assert ($refreshed_output.stderr | str contains "tokens") "the resolved referent should include the refreshed token"
assert ($refreshed_output.stderr != $first_output.stderr) "the refreshed token should be new"

advance_time $remote 2sec
advance_time $local 2sec

let pid = open ($remote.directory | path join lock) | into int
kill --signal 2 $pid
wait_for_exit $pid "the remote should stop"

let cached_output = with-env { TANGRAM_QUIET: "false" } { tg --url $local.url get --cached foo | complete }
success $cached_output
let cached = $cached_output.stdout | from json
assert (($cached | get --optional tokens) == null) "cached get should not print tokens to stdout"
assert not ($cached_output.stderr | str contains "tokens") "the resolved referent should omit an expired token"
