use ../lib/test.nu *

# Capture must finish verification before preserving a partial exact token's permissions.
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
	verification: { permissions: { initial: false } }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode
let missing = 'fil_010000000000000000000000000000000000000000000000000000'
let expression = 'tg.directory({"file":tg.file({"contents":"best","dependencies":{"missing":{"node":MISSING}}})})'
	| str replace MISSING $missing
let directory = tg --token $alice.token put $expression | str trim
let file = tg --token $alice.token children $directory | from json | get 0 | split row '?' | first
tg --token $root_token index
let object = http get --headers { Accept: application/json, Authorization: $'Bearer ($alice.token)' } --unix-socket $socket $'http://localhost/objects/($file)'
let node_token = $object.tokens.local.0
assert equal (token-body $node_token).permissions [object_node]
tg --token $root_token grant $alice.user.id object_subtree $directory | ignore
tg --token $root_token index
let object = http get --headers { Accept: application/json, Authorization: $'Bearer ($alice.token)' } --unix-socket $socket $'http://localhost/objects/($directory)'
let subtree_token = $object.tokens.local.0
assert ('object_subtree' in (token-body $subtree_token).permissions)

let params = { resource: $file } | to json --raw
let written = tg --token $root_token checkpoint watch permission_capture.written --params $params | from json | get watch
let source = '
	export default function () {
		return tg.File.withReferent({
			node: "FILE",
			options: { tokens: { local: ["NODE_TOKEN", "SUBTREE_TOKEN"] } },
		});
	}
' | str replace FILE $file | str replace NODE_TOKEN $node_token | str replace SUBTREE_TOKEN $subtree_token
let module = artifact { tangram.ts: $source }
let process = tg --token $alice.token build --detach $module | str trim
let finished = tg --token $alice.token wait $process | from json
assert equal $finished.exit 0
let hit = timeout 30s tg --token $root_token checkpoint wait permission_capture.written $written 0 | from json
assert equal $hit.params.process $process

# Hold capture before descent, then remove temporary permissions and delegations.
tg --token $alice.token grant $bob.user.id process_node_output_objects $process | ignore
tg --token $root_token tag put retained $process
tg --token $root_token index
advance_time $local 2min
tg --token $root_token clean
tg --token $root_token index
let object = http get --headers { Accept: application/json, Authorization: $'Bearer ($bob.token)' } --unix-socket $socket $'http://localhost/objects/($file)'
assert ('object_subtree' in (token-body $object.tokens.local.0).permissions) 'capture must preserve the subtree proof found after its initial node proof'
tg --token $root_token checkpoint unwatch permission_capture.written $written
