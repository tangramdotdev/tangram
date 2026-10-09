use ../lib/test.nu *

# Local pulls return bounded capabilities for the permissions they actually verify.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } },
	object: { permission_time_to_live: 3600 },
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let object = tg --token $alice.token put 'tg.file("local private object")' | str trim
let id = $object | referent node
let original = $object | referent tokens local | first | token body

# Advancing time ensures a fresh full lifetime would outlive the supplied proof.
sleep 2sec
failure (tg --token $bob.token pull $id | complete)
let output = tg --no-quiet --token $bob.token pull $object | complete
success $output
assert equal ($output.stdout | referent node) $id
assert not ($output.stderr | str contains 'tokens[') "a local pull should not start a sync"
let tokens = $output.stdout | referent tokens local
assert equal ($tokens | length) 1
let body = $tokens.0 | token body
assert equal $body.resource $id
assert equal $body.permissions [object_subtree]
assert ($body.expires_at <= $original.expires_at) "the new token must not outlive the accepted proof"
success (tg --token $bob.token get ($output.stdout | str trim) | complete)

# A shallow local process pull must take the local fast path with a get capability.
let path = artifact { tangram.ts: 'export default () => "done";' }
let process = tg --token $alice.token spawn $path | referent node
tg --token $alice.token wait $process | ignore
tg index
wait_until {
	(tg --token $alice.token pull --no-tokens --no-process-error-objects --no-process-output-objects $process | complete | get exit_code) == 0
} --timeout 15sec "the process should be stored and locally authorized"
let socket = $local.url | str replace 'http+unix://' '' | url decode
let response = http get --unix-socket $socket --headers { Authorization: $'Bearer ($alice.token)' } $'http://localhost/processes/($process)'
let query = { 'tokens[local][0]': $response.tokens.local.0 } | url build-query
let referent = $'($process)?location=local&($query)'
assert equal ($response.tokens.local.0 | token body | get permissions) [process_node process_parent]
let output = tg --no-quiet --token $alice.token pull --local --no-process-error-objects --no-process-output-objects $referent | complete
success $output
assert not ($output.stderr | str contains 'tokens[') "the shallow process pull should take the local fast path"
let tokens = $output.stdout | referent tokens local
assert equal ($tokens | length) 1
let body = $tokens.0 | token body
assert equal $body.resource $process
assert equal $body.permissions [process_node]
