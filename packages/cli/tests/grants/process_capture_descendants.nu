use ../lib/test.nu *

# Process output capture preserves directory and file nodes whose dependency cannot be read.

def token-body [token: string] {
	$token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
}

let root_token = random chars
let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	indexer: { permission_capture: { delegation_time_to_live: 60 } }
	object: { permission_time_to_live: 60 }
	process: { await_push: false }
	sync: { permission_time_to_live: 60 }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode
let missing = 'fil_010000000000000000000000000000000000000000000000000000'

# Compute the expected ids so the descendant checkpoint is installed before the process finishes.
let expression = 'tg.directory({"file":tg.file({"contents":"capture","dependencies":{"missing":{"node":MISSING}}})})'
	| str replace MISSING $missing
let directory = tg --token $alice.token put --no-tokens $expression | referent node
let file = tg --token $alice.token children $directory | from json | get 0 | split row '?' | first
tg --token $root_token index
for id in [$directory $file] {
	let object = http get --headers { Accept: application/json, Authorization: $'Bearer ($alice.token)' } --unix-socket $socket $'http://localhost/objects/($id)'
	assert equal (token-body $object.tokens.local.0).permissions [object_node]
}
failure (tg --token $bob.token get --bytes $directory | complete) "the reader must not already have directory access."
failure (tg --token $bob.token get --bytes $file | complete) "the reader must not already have file access."
failure (tg --token $root_token get --bytes $missing | complete) "the dependency must be unavailable even to root."

let params = { resource: $file } | to json --raw
let write = tg --token $root_token checkpoint watch permission_capture.write --params $params | from json | get watch
let written = tg --token $root_token checkpoint watch permission_capture.written --params $params | from json | get watch
let source = '
	export default function () {
		return tg.directory({
			file: tg.file("capture").dependencies({ missing: tg.File.withId("MISSING") }),
		});
	}
' | str replace MISSING $missing
let module = artifact { tangram.ts: $source }
let process = tg --token $alice.token build --no-tokens --detach $module | referent node
let finished = tg --token $alice.token wait $process | from json
assert equal $finished.exit 0
assert equal ($finished.output.value | split row '?' | first) $directory

tg --token $alice.token grant $bob.user.id process_node_output_objects $process | ignore
tg --token $root_token index
let hit = timeout 30s tg --token $root_token checkpoint wait permission_capture.write $write 0 | from json
assert equal $hit.params.process $process
tg --token $root_token checkpoint continue permission_capture.write $write 0
timeout 30s tg --token $root_token checkpoint wait permission_capture.written $written 0 | ignore
tg --token $root_token checkpoint unwatch permission_capture.write $write
tg --token $root_token checkpoint unwatch permission_capture.written $written

# Remove temporary proofs and delegations, leaving only the captured process permissions.
tg --token $root_token tag put retained $process
tg --token $root_token index
advance_time $local 2min
tg --token $root_token clean
for id in [$directory $file] {
	success (tg --token $root_token get --bytes $id | complete) "the privately retained process must keep its output bytes available."
	success (tg --token $bob.token get --bytes $id | complete) "the output aspect must expose permanently captured node permissions."
}
failure (tg --token $bob.token get --bytes $missing | complete) "the output aspect must not invent access to the unavailable dependency."
failure (tg --token $bob.token get $directory --depth inf | complete) "node output permissions must respect the process's lack of subtree permission."
failure (tg --token $bob.token get $process | complete) "the output-only grant must not expose the process node."
