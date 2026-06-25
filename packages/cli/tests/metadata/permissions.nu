use ../../test.nu *

# Metadata is masked by the indexed grants for the authenticated principal.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

let directory = tg --token $alice.token put 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | str trim
tg --token $alice.token index

let alice_object_metadata = tg --token $alice.token metadata $directory | from json
assert equal $alice_object_metadata.subtree.count 5 "Alice should see the object subtree metadata after indexing."
assert ($alice_object_metadata.subtree.size > $alice_object_metadata.node.size) "Alice should see the full object subtree metadata."

let output = tg --token $bob.token metadata $directory | complete
failure $output "Bob should not be able to get Alice's object metadata without a grant."
snapshot ($output.stderr | redact | normalize_ids) '
	error an error occurred
	-> failed to find the object metadata
	   id = dir_010000000000000000000000000000000000000000000000000000

'

let child = "pcs_00081061050r3gg28a1c60t3gf20"
let parent = "pcs_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0"
let process_data = {
	command: "cmd_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0",
	created_at: 0,
	finished_at: 0,
	host: "test",
	sandbox: "sbx_00041061050r3gg28a1c60t3gf20",
	status: "finished",
}

tg --token $alice.token process put $child ($process_data | merge { children: [] } | to json)
tg --token $alice.token process put $parent ($process_data | merge {
	children: [
		{
			cached: false,
			options: {},
			process: $child,
		},
	],
} | to json)
tg --token $alice.token index

let alice_process_metadata = tg --token $alice.token metadata $parent | from json
assert equal $alice_process_metadata.subtree.count 2 "Alice should see the process subtree metadata after indexing."
assert equal ($alice_process_metadata.node? | default {} | columns) [] "Alice should not see process node aspect metadata from a process_node grant."

let output = tg --token $bob.token metadata $parent | complete
failure $output "Bob should not be able to get Alice's process metadata without a grant."
snapshot ($output.stderr | redact | normalize_ids) '
	error an error occurred
	-> failed to find the process metadata
	   id = <process>

'
