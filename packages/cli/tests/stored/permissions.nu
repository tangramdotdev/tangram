use ../../test.nu *

# Storage status is masked by the authenticated principal's permissions.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

let directory = tg --token $alice.token put 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | str trim
tg --token $alice.token index

let alice_stored = tg --token $alice.token stored $directory | from json
assert equal $alice_stored.subtree true "the owner should see that the object subtree is stored"

let output = tg --token $bob.token stored $directory | complete
failure $output "a principal without object permissions should not see the storage status"

tg --token $alice.token grant $bob.user.id object_node $directory | ignore
let bob_node_stored = tg --token $bob.token stored $directory | from json
assert equal ($bob_node_stored | columns) [] "an object node grant should mask the subtree storage status"

tg --token $alice.token grant $bob.user.id object_subtree $directory | ignore
let bob_subtree_stored = tg --token $bob.token stored $directory | from json
assert equal $bob_subtree_stored.subtree true "an object subtree grant should reveal the subtree storage status"

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

let alice_process_stored = tg --token $alice.token stored $parent | from json
assert equal $alice_process_stored.subtree true "the owner should see that the process subtree is stored"

let output = tg --token $bob.token stored $parent | complete
failure $output "a principal without process permissions should not see the storage status"

tg --token $alice.token grant $bob.user.id process_node $parent | ignore
let bob_node_stored = tg --token $bob.token stored $parent | from json
assert equal ($bob_node_stored | columns) [] "a process node grant should mask the subtree storage status"

tg --token $alice.token grant $bob.user.id process_subtree $parent | ignore
let bob_subtree_stored = tg --token $bob.token stored $parent | from json
assert equal $bob_subtree_stored.subtree true "a process subtree grant should reveal the subtree storage status"
