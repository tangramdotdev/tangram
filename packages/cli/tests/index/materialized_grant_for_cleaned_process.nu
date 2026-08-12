use ../../test.nu *

# A non-expiring materialized grant must not survive after a supporting explicit grant is revoked while the process is absent from the index.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

let child = "pcs_00081061050r3gg28a1c60t3gf20"
let parent = "pcs_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0"
let process_data = {
	command: "cmd_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0",
	created_at: 0,
	finished_at: 0,
	host: "x86_64-linux",
	sandbox: "sbx_00041061050r3gg28a1c60t3gf20",
	status: "finished",
}
let child_data = $process_data | merge { children: [] }
let parent_data = $process_data | merge {
	children: [
		{
			cached: false,
			options: {},
			process: $child,
		},
	],
}

tg --token $alice.token process put $child ($child_data | to json)
tg --token $alice.token process put $parent ($parent_data | to json)

# These explicit grants imply a non-expiring process_subtree grant on the parent.
tg --token $alice.token grant $eve.user.id process_node $parent | ignore
tg --token $alice.token grant $eve.user.id process_subtree $child | ignore
tg --token $alice.token index

let metadata = tg --token $eve.token metadata $parent | from json
assert equal $metadata.subtree.count 2 "the explicit grants should materialize a subtree grant on the parent"

# Remove the process graph, then revoke one of the grants supporting the materialized grant.
tg --token $alice.token clean
tg --token $alice.token revoke $eve.user.id process_subtree $child | ignore
tg --token $alice.token index

# Restore the same graph without restoring the revoked grant.
tg --token $alice.token process put $child ($child_data | to json)
tg --token $alice.token process put $parent ($parent_data | to json)
tg --token $alice.token index

let metadata = tg --token $eve.token metadata $parent | from json
assert equal ($metadata.subtree?.count? | default null) null "the stale materialized subtree grant must not survive restoration"
