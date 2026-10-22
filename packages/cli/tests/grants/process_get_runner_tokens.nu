use ../lib/test.nu *

# Runner reads return child and object tokens that let a user without grants read the process's command, output, and child.
def get-process [socket: string, bearer: string, process: string, token: string, metadata: bool = false, availability: bool = false] {
	let query = { metadata: $metadata, availability: $availability, source: runner }
	let query = if ($token | is-empty) { $query } else { $query | insert 'tokens[local][0]' $token }
	let query = $query | url build-query
	http get --max-time 10sec --unix-socket $socket --headers { Authorization: $'Bearer ($bearer)' } $'http://localhost/processes/($process)?($query)'
}
def module-referent [command: record] {
	let referent = $command.node.args.3.value.value.referent
	$'($referent.node)?tokens[local][0]=($referent.options.tokens.local.0 | url encode --all)'
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
assert ($baseline.data.children.0.process | referent tokens local | is-empty) 'a node reader must not receive the stored child authorization tokens'
for flags in [[false false] [true false] [false true] [true true]] {
	let result = get-process $socket $stranger.token $process $token $flags.0 $flags.1
	let child_token = $result.data.children.0.process | referent tokens local | first
	let child = get-process $socket $stranger.token ($result.data.children.0.process | referent node) $child_token
	success (tg --token $stranger.token get --bytes $child.data.output.node.value | complete) 'the child token must authorize the child output'
	success (tg --token $stranger.token get --bytes (module-referent $result.data.command) | complete) 'the command token must authorize the command objects'
	success (tg --token $stranger.token get --bytes $result.data.output.node.value | complete) 'the output token must authorize the output'
	if $flags.0 { assert ('output_objects' in ($result.metadata.node? | default {} | columns)) }
	if $flags.1 { assert ('node_output_objects' in ($result.availability | columns)) }
}

for flags in [[false false] [true false] [false true] [true true]] {
	let result = get-process $socket $stranger.token $process $scoped $flags.0 $flags.1
	success (tg --token $stranger.token get --bytes (module-referent $result.data.command) | complete) 'the scoped token must authorize the command objects'
	success (tg --token $stranger.token get --bytes $result.data.output.node.value | complete) 'the scoped token must authorize the output'
	assert equal $result.data.children.0.process $baseline.data.children.0.process
	assert equal $result.data.log? $baseline.data.log?
	if $flags.0 {
		assert not ('output_objects' in ($result.metadata.subtree? | default {} | columns))
		assert not ('log_objects' in ($result.metadata.node? | default {} | columns))
	}
	if $flags.1 { assert not ('node_log_objects' in ($result.availability | columns)) }
}
tg --token $root checkpoint unwatch runner.process.control.retention.finished $retention
