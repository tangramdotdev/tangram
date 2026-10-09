use ../lib/test.nu *

# Process get derives scoped tokens without searches, including metadata and availability reads.
def token-body [token: string] {
	$token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
}
def get-process [socket: string, user_token: string, process: string, token: string = '', metadata: bool = false, availability: bool = false] {
	let query = { metadata: $metadata, availability: $availability, source: index }
	let query = if ($token | is-empty) { $query } else { $query | insert 'tokens[local][0]' $token }
	let query = $query | url build-query
	http get --unix-socket $socket --headers { Authorization: $'Bearer ($user_token)' } $'http://localhost/processes/($process)?($query)'
}
def put-process [socket: string, root: string, process: string, data: record] {
	http put --raw --unix-socket $socket --headers { Authorization: $'Bearer ($root)' } --content-type application/json $'http://localhost/processes/($process)' { data: $data } | ignore
}
def object-token [referent: string] {
	let tokens = $referent | referent tokens local
	assert not ($tokens | is-empty) $'missing tokens for ($referent)'
	$tokens | first
}
let root = random chars
let local = server spawn --now '2026-01-01T00:00:00Z' --preserve-keys --config {
	authentication: { root: { token: $root }, users: { providers: { insecure: true } } }
	object: { permission_time_to_live: 60 }
	process: { permission_time_to_live: 60 }
}
let owner = tg login --verbose --name owner | from json
let reader = tg login --verbose --name reader | from json
let stranger = tg login --verbose --name stranger | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode
let command = tg --token $root put --no-tokens 'tg.command({"executable":"/bin/true","host":"x86_64-linux"})' | referent node
let output = tg --token $root put --no-tokens 'tg.directory({"child":tg.file("output")})' | referent node
let log = tg --token $root put --no-tokens 'tg.blob("log")' | referent node
let error = 'err_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0'
let child = 'pcs_00081061050r3gg28a1c60t3gf20'
let parent = 'pcs_00081061050r3gg28a1c60t3gf40'
let data = { children: [], command: $command, error: $error, created_at: 0, exit: 0, finished_at: 1, host: 'x86_64-linux', log: $log, output: { kind: object, value: $output }, status: finished }
put-process $socket $root $child $data
put-process $socket $root $parent ($data | upsert children [{ cached: false, process: $child }])
for permission in [process_node process_node_command_objects process_node_output_objects] {
	tg --token $root grant $reader.user.id $permission $parent | ignore
}
tg --token $root grant $owner.user.id process_parent $parent | ignore
tg --token $root index
let scoped = (get-process $socket $reader.token $parent '' true true).tokens.local.0
let full = (get-process $socket $owner.token $parent '' true true).tokens.local.0
assert ('process_node_command_objects' in (token-body $scoped).permissions)
assert ('process_parent' in (token-body $full).permissions)

let inline = 'pcs_00081061050r3gg28a1c60t3gf60'
let inline_command = { node: {
	args: [{ kind: value, value: { kind: object, value: $output } }],
	env: { INPUT: { kind: value, value: { kind: object, value: $output } } },
	executable: { node: { path: '/bin/true' } },
	host: 'x86_64-linux',
	stdin: { node: $log },
} }
put-process $socket $root $inline ($data | upsert command $inline_command)
tg --token $root grant $owner.user.id process_parent $inline | ignore
let inline_token = (get-process $socket $owner.token $inline '' true).tokens.local.0
server stop $local
let config = open $local.config_path
let searches = {
	ancestor: { max_depth: 0, max_edges: 0, max_nodes: 0 }
	descendant: { max_depth: 0, max_edges: 0, max_nodes: 0 }
	subtree: { max_depth: 0, max_objects: 0, max_processes: 0 }
}
$config | upsert verification.permissions { initial: $searches, final: $searches } | to json | save --force $local.config_path
let local = server start $local
for flags in [[false false] [true false] [false true] [true true]] {
	let result = get-process $socket $stranger.token $parent $scoped $flags.0 $flags.1
	let permissions = (token-body $result.tokens.local.0).permissions
	assert ('process_node_command_objects' in $permissions)
	assert ('process_node_output_objects' in $permissions)
	assert not ('process_node_log_objects' in $permissions)
	let command_token = object-token $result.data.command
	assert equal (token-body $command_token).resource $command
	assert ('object_subtree' in (token-body $command_token).permissions) 'node command permissions must authorize the command object subtree'
	let output_token = object-token $result.data.output.value
	assert equal (token-body $output_token).resource $output
	assert ('object_subtree' in (token-body $output_token).permissions)
	assert ((token-body $output_token).expires_at <= (token-body $scoped).expires_at)
	assert not ($result.data.log | str contains 'tokens')
	assert not ($result.data.error | str contains 'tokens')
	assert not ($result.data.children.0.process | str contains 'tokens')
	if $flags.0 {
		assert not ('command_objects' in ($result.metadata.subtree? | default {} | columns))
		assert not ('log_objects' in ($result.metadata.node? | default {} | columns))
	}
	if $flags.1 {
		assert not ('subtree_log_objects' in ($result.availability | columns))
	}
}
let result = get-process $socket $stranger.token $parent $full
let log_token = object-token $result.data.log
assert equal (token-body $log_token).resource $log
assert ('object_subtree' in (token-body $log_token).permissions)
let error_token = object-token $result.data.error
assert equal (token-body $error_token).resource $error
assert ('object_subtree' in (token-body $error_token).permissions)
let child_token = object-token $result.data.children.0.process
assert equal (token-body $child_token).resource $child
assert ('process_subtree' in (token-body $child_token).permissions)
assert ('process_subtree_command_objects' in (token-body $child_token).permissions)
assert not ('process_parent' in (token-body $child_token).permissions)
assert ((token-body $child_token).expires_at <= (token-body $full).expires_at)
let child_result = get-process $socket $stranger.token $child $child_token
assert ('object_subtree' in (token-body (object-token $child_result.data.output.value)).permissions)
success (tg --token $stranger.token get --bytes $result.data.command | complete) 'the derived command token must work without a search'
success (tg --token $stranger.token get --bytes $result.data.output.value | complete) 'the derived output token must work without a search'


let result = get-process $socket $stranger.token $inline $inline_token
for referent in [$result.data.command.node.args.0.value.value $result.data.command.node.env.INPUT.value.value] {
	assert ('object_subtree' in (token-body (object-token $referent)).permissions) 'inline command object references must receive subtree tokens'
}
assert ('object_subtree' in (token-body $result.data.command.node.stdin.options.tokens.local.0).permissions)
