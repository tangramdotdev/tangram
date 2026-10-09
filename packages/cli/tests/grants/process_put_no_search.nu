use ../lib/test.nu *

# Putting a process records permissions authorized by supplied tokens without searching for object or process permissions.
let searches = {
	ancestor: { max_depth: 0, max_edges: 0, max_nodes: 0 }
	descendant: { max_depth: 0, max_edges: 0, max_nodes: 0 }
	subtree: { max_depth: 0, max_objects: 0, max_processes: 0 }
}
let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	verification: { permissions: { initial: $searches, final: $searches } }
}
let alice = tg login --verbose --name alice | from json
let process = "pcs_00081061050r3gg28a1c60t3gf20"
let data = {
	children: [],
	command: "cmd_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0",
	created_at: 0,
	error: "err_010000000000000000000000000000000000000000000000000000",
	exit: 0,
	finished_at: 1,
	host: "x86_64-linux",
	log: "blb_010000000000000000000000000000000000000000000000000000",
	output: { kind: "object", value: "fil_010000000000000000000000000000000000000000000000000000" },
	status: "finished",
}
let socket = $local.url | str replace 'http+unix://' '' | url decode
let response = http put --raw --unix-socket $socket --headers { Authorization: $'Bearer ($alice.token)' } --content-type application/json $'http://localhost/processes/($process)' { data: $data } | from json
assert (($response | get --optional tokens.local | default [] | length) > 0) "the put must return a node token without searching"
let body = $response.tokens.local.0 | split row '.' | get 1 | decode base64 | decode utf-8 | from json
assert equal $body.permissions [process_node] "references without authorizing tokens must not confer field permissions"
