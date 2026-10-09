use ../lib/test.nu *

# Runner reads derive child and object tokens without authorization searches.
def token-body [token: string] {
	$token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
}
def get-process [socket: string, bearer: string, process: string, token: string, metadata: bool = false, availability: bool = false] {
	let query = { metadata: $metadata, availability: $availability, source: runner }
	let query = if ($token | is-empty) { $query } else { $query | insert 'tokens[local][0]' $token }
	let query = $query | url build-query
	http get --max-time 10sec --unix-socket $socket --headers { Authorization: $'Bearer ($bearer)' } $'http://localhost/processes/($process)?($query)'
}
def referent-token [referent: string] {
	$referent | referent tokens local | first
}
def command-referent [command: any] {
	if ($command | describe) == string { return $command }
	let token = $command.options.tokens.local.0
	$'((token-body $token).resource)?tokens[local][0]=($token | url encode --all)'
}
let root = random chars
let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root }, users: { providers: { insecure: true } } }
	object: { permission_time_to_live: 60 }
	process: { permission_time_to_live: 60 }
	runner: { process_state_ttl: 60, sandbox_state_ttl: 60 }
}
let reader = tg login --verbose --name reader | from json
let stranger = tg login --verbose --name stranger | from json
let path = artifact {
	tangram.ts: 'export default async () => await tg.build(child); export function child() { return tg.file("runner-output"); }'
}
let retention = tg --token $root checkpoint watch runner.process.control.retention.finished | from json | get watch
let spawned = tg --token $root build --detach --verbose $path | from json
let process = $spawned.process | referent node
let token = $spawned.tokens.local.0
tg --token $root wait --source=index $process | ignore
tg --token $root index
let socket = $local.url | str replace 'http+unix://' '' | url decode
for permission in [process_node process_node_command_objects process_node_output_objects] {
	tg --token $root grant $reader.user.id $permission $process | ignore
}
tg --token $root index
let baseline = get-process $socket $reader.token $process '' true true
let scoped = $baseline.tokens.local.0
assert not ($baseline.data.children.0.process | referent tokens local | is-empty) 'the runner must preserve the child tokens already in its state'
advance_time $local 10sec
let watch = tg --token $root checkpoint watch verification.index | from json | get watch
for flags in [[false false] [true false] [false true] [true true]] {
	let result = get-process $socket $stranger.token $process $token $flags.0 $flags.1
	assert equal ($result.tokens.local | length) 1
	let returned = token-body $result.tokens.local.0
	assert ('process_parent' in $returned.permissions)
	assert ($returned.expires_at <= (token-body $token).expires_at)
	let command = command-referent $result.data.command
	let command_token = referent-token $command
	assert ('object_subtree' in (token-body $command_token).permissions)
	assert ((token-body $command_token).expires_at <= (token-body $token).expires_at)
	let output_token = referent-token $result.data.output.value
	assert ('object_subtree' in (token-body $output_token).permissions)
	assert ((token-body $output_token).expires_at <= (token-body $token).expires_at)
	let child_token = referent-token $result.data.children.0.process
	assert (('process_subtree' in (token-body $child_token).permissions) or ('process_parent' in (token-body $child_token).permissions))
	assert (('process_subtree_output_objects' in (token-body $child_token).permissions) or ('process_parent' in (token-body $child_token).permissions))
	if not ($baseline.data.children.0.process | str contains 'tokens') {
		assert not ('process_parent' in (token-body $child_token).permissions)
	}
	assert ((token-body $child_token).expires_at <= (token-body $token).expires_at)
	let child = get-process $socket $stranger.token ($result.data.children.0.process | referent node) $child_token
	assert ('object_subtree' in (token-body (referent-token $child.data.output.value)).permissions)
	success (tg --token $stranger.token get --bytes $command | complete)
	success (tg --token $stranger.token get --bytes $result.data.output.value | complete)
	if $flags.0 { assert ('output_objects' in ($result.metadata.node? | default {} | columns)) }
	if $flags.1 { assert ('node_output_objects' in ($result.availability | columns)) }
}

for flags in [[false false] [true false] [false true] [true true]] {
	let result = get-process $socket $stranger.token $process $scoped $flags.0 $flags.1
	let permissions = (token-body $result.tokens.local.0).permissions
	assert ('process_node_command_objects' in $permissions)
	assert ('process_node_output_objects' in $permissions)
	assert not ('process_parent' in $permissions)
	assert not ('process_node_log_objects' in $permissions)
	assert ('object_subtree' in (token-body (referent-token (command-referent $result.data.command))).permissions)
	assert ('object_subtree' in (token-body (referent-token $result.data.output.value)).permissions)
	assert equal $result.data.children.0.process $baseline.data.children.0.process
	assert equal $result.data.log? $baseline.data.log?
	if $flags.0 {
		assert not ('output_objects' in ($result.metadata.subtree? | default {} | columns))
		assert not ('log_objects' in ($result.metadata.node? | default {} | columns))
	}
	if $flags.1 { assert not ('node_log_objects' in ($result.availability | columns)) }
}
tg --token $root checkpoint unwatch verification.index $watch
tg --token $root checkpoint unwatch runner.process.control.retention.finished $retention
