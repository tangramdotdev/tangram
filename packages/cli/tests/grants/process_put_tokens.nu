use ../lib/test.nu *

# Process put preserves permissions authorized by supplied tokens without authorization searches or widening their scope.
def token-body [token: string] {
	$token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
}

def get-object [socket: string, token: string, id: string] {
	http get --headers { Accept: application/json, Authorization: $'Bearer ($token)' } --unix-socket $socket $'http://localhost/objects/($id)'
}

def put-process [socket: string, token: string, id: string, command: string, output: string] {
	let data = {
		children: [],
		command: $command,
		created_at: 0,
		exit: 0,
		finished_at: 1,
		host: "x86_64-linux",
		output: { kind: "object", value: $output },
		status: "finished",
	}
	let response = http put --raw --unix-socket $socket --headers { Authorization: $'Bearer ($token)' } --content-type application/json $'http://localhost/processes/($id)' { data: $data } | from json
	assert equal (token-body $response.tokens.local.0).permissions [process_node]
}

let root = random chars
let local = server spawn --now '2026-01-01T00:00:00Z' --preserve-keys --config {
	authentication: { root: { token: $root }, users: { providers: { insecure: true } } }
	object: { permission_time_to_live: 60 }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let reader = tg login --verbose --name reader | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode
let command = tg --token $root put --no-tokens 'tg.command({"executable":"/bin/true","host":"x86_64-linux"})' | referent node
let subtree = tg --token $root put --no-tokens 'tg.file("subtree token")' | referent node
let node = tg --token $root put --no-tokens 'tg.file("node token")' | referent node
let expired = tg --token $root put --no-tokens 'tg.file("expired token")' | referent node
let wrong = tg --token $root put --no-tokens 'tg.file("wrong resource")' | referent node
tg --token $root grant $alice.user.id object_node $node | ignore
tg --token $root index
let command_token = (get-object $socket $root $command).tokens.local.0
let subtree_token = (get-object $socket $root $subtree).tokens.local.0
let node_object = get-object $socket $alice.token $node
let node_token = $node_object.tokens.local.0
let expired_token = (get-object $socket $root $expired).tokens.local.0
let node_child = (get-object $socket $root $node).children | columns | first
assert ('object_subtree' in (token-body $subtree_token).permissions)
assert equal (token-body $node_token).permissions [object_node]

# Disable searches for every put, including objects with tokens that authorize partial permissions or are unusable.
server stop $local
let config = open $local.config_path
let searches = {
	ancestor: { max_depth: 0, max_edges: 0, max_nodes: 0 }
	descendant: { max_depth: 0, max_edges: 0, max_nodes: 0 }
	subtree: { max_depth: 0, max_objects: 0, max_processes: 0 }
}
$config | upsert verification.permissions { initial: $searches, final: $searches } | to json | save --force $local.config_path
let local = server start $local
let command_referent = $'($command)?tokens[local][0]=($command_token | url encode --all)'
let subtree_process = "pcs_00081061050r3gg28a1c60t3gf20"
let node_process = "pcs_00081061050r3gg28a1c60t3gf40"
let expired_process = "pcs_00081061050r3gg28a1c60t3gf60"
let wrong_process = "pcs_00081061050r3gg28a1c60t3gf80"
put-process $socket $bob.token $subtree_process $command_referent $'($subtree)?tokens[local][0]=($subtree_token | url encode --all)'
put-process $socket $bob.token $node_process $command_referent $'($node)?tokens[local][0]=($node_token | url encode --all)'

advance_time $local 2min
let command_token = (get-object $socket $root $command).tokens.local.0
let command_referent = $'($command)?tokens[local][0]=($command_token | url encode --all)'
put-process $socket $bob.token $expired_process $command_referent $'($expired)?tokens[local][0]=($expired_token | url encode --all)'
put-process $socket $bob.token $wrong_process $command_referent $'($wrong)?tokens[local][0]=($command_token | url encode --all)'

# Read through process field grants after the original object tokens have expired.
server stop $local
$config | to json | save --force $local.config_path
let local = server start $local
for process in [$subtree_process $node_process $expired_process $wrong_process] {
	tg --token $root grant $reader.user.id process_node_command_objects $process | ignore
	tg --token $root grant $reader.user.id process_node_output_objects $process | ignore
}
tg --token $root index
success (tg --token $reader.token get --bytes $command | complete) "the permissions authorized by the command token must be preserved"
assert equal (tg --token $reader.token cat $subtree | str trim) "subtree token" "the permissions authorized by the subtree token must be preserved"
success (tg --token $reader.token get --bytes $node | complete) "the permissions authorized by the node token must be preserved"
failure (tg --token $reader.token get --bytes $node_child | complete) "a node token must not grant access to descendants"
failure (tg --token $reader.token get --bytes $expired | complete) "an expired token must not grant access"
failure (tg --token $reader.token get --bytes $wrong | complete) "a token for another resource must not grant access"
