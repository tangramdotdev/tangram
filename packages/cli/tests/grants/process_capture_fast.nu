use ../lib/test.nu *

# Exact command and output subtree proofs preserve permissions without capture work or a command authorization search.

def token-body [token: string] {
	$token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
}

let root_token = random chars
let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	indexer: { permission_capture: { delegation_time_to_live: 60, poll_interval: 0.05 } }
	object: { permission_time_to_live: 60 }
	process: { await_push: false }
	sync: { permission_time_to_live: 60 }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let reader = tg login --verbose --name reader | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode
let directory = tg --token $alice.token put 'tg.directory({"file":tg.file("fast")})' | str trim
tg --token $root_token index
let object = http get --headers { Accept: application/json, Authorization: $'Bearer ($alice.token)' } --unix-socket $socket $'http://localhost/objects/($directory)'
let output_token = $object.tokens.local.0
assert ('object_subtree' in (token-body $output_token).permissions)
let source = '
	export function passthrough(id: tg.Directory.Id, token: string) {
		return tg.Directory.withReferent({ node: id, options: { tokens: { local: [token] } } });
	}
	export default function () {
		return tg.command(passthrough, "DIRECTORY", "TOKEN");
	}
' | str replace DIRECTORY $directory | str replace TOKEN $output_token
let module = artifact { tangram.ts: $source }
let command = tg --token $alice.token build $module | str trim | split row '?' | first
tg --token $root_token index
let command_object = tg --token $alice.token children $command | from json | get 0 | split row '?' | first
failure (tg --token $bob.token get --bytes $command | complete) "the spawn caller must need the supplied command token."
let object = http get --headers { Accept: application/json, Authorization: $'Bearer ($alice.token)' } --unix-socket $socket $'http://localhost/objects/($command)'
let token = $object.tokens.local.0
let proof = token-body $token
assert equal $proof.resource $command
assert ('object_subtree' in $proof.permissions)

let params = { resource: $command } | to json --raw
let authorization = tg --token $root_token checkpoint watch verification.index --params $params | from json | get watch
let capture = tg --token $root_token checkpoint watch permission_capture.started | from json | get watch
let arg = {
	cached: false
	command: { node: $command, options: { tokens: { local: [$token] } } }
	sandbox: {}
	stderr: 'null'
	stdin: 'null'
	stdout: 'null'
} | to json --raw
let response = http post --raw --max-time 30sec --unix-socket $socket --headers { 'Content-Type': application/json, Authorization: $'Bearer ($bob.token)' } 'http://localhost/processes/spawn' $arg
assert (not ($response | str contains 'event: error')) "the exact command proof must authorize spawn."
let spawned = $response | lines | where { $in starts-with 'data: ' } | last | str substring 6.. | from json
let process = $spawned.process | split row '?' | first
tg --token $root_token checkpoint unwatch verification.index $authorization
let finished = timeout 30s tg --token $bob.token wait $process | from json
assert ($finished.exit == 0) ($finished | to json)
let output = $finished.output.value | split row '?' | first
assert equal $output $directory
tg --token $root_token index
let unexpected = timeout 5s tg --token $root_token checkpoint wait permission_capture.started $capture 0 | complete
assert equal $unexpected.exit_code 124 "spawn and finish must not enqueue capture for roots covered by subtree proofs."
tg --token $root_token checkpoint unwatch permission_capture.started $capture

# Permanent process permissions must outlive the proofs used by both operations.
tg --token $bob.token grant $reader.user.id process_node_command_objects $process | ignore
tg --token $bob.token grant $reader.user.id process_node_output_objects $process | ignore
tg --token $root_token tag put retained $process
tg --token $root_token index
success (tg --token $reader.token get --bytes $command_object | complete) "the command aspect must expose the actual command input."
advance_time $local 2min
tg --token $root_token clean
success (tg --token $root_token get --bytes $command_object | complete) "the command input bytes must remain available after cleaning."
success (tg --token $reader.token get $output --depth inf | complete) "the output proof must have been preserved permanently."
success (tg --token $reader.token get --bytes $command_object | complete) "the supplied command proof must have been preserved permanently."
