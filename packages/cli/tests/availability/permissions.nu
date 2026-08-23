use ../../test.nu *

# Availability is masked by the authenticated principal's permissions.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

let directory = tg --token $alice.token put 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | str trim
tg --token $alice.token index

let alice_availability = tg --token $alice.token availability $directory | from json
assert equal $alice_availability.subtree true "the owner should see that the object subtree is available"

let output = tg --token $bob.token availability $directory | complete
failure $output "a principal without object permissions should not see the availability"

tg --token $alice.token grant $bob.user.id object_node $directory | ignore
let bob_node_availability = tg --token $bob.token availability $directory | from json
assert equal ($bob_node_availability | columns) [] "an object node grant should mask the subtree availability"

tg --token $alice.token grant $bob.user.id object_subtree $directory | ignore
let bob_subtree_availability = tg --token $bob.token availability $directory | from json
assert equal $bob_subtree_availability.subtree true "an object subtree grant should reveal the subtree availability"

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

let alice_process_availability = tg --token $alice.token availability $parent | from json
assert equal $alice_process_availability.subtree true "the owner should see that the process subtree is available"

let derived_fields = [node_error node_log node_output subtree_error subtree_log subtree_output]
let bob_derived_availability = tg --token $bob.token availability $parent | from json
assert equal ($bob_derived_availability | columns) $derived_fields "complete process aspects with no objects should reveal their availability"

tg --token $alice.token grant $bob.user.id process_node $parent | ignore
let bob_node_availability = tg --token $bob.token availability $parent | from json
assert equal ($bob_node_availability | columns) $derived_fields "a process node grant should still mask the general subtree availability"

tg --token $alice.token grant $bob.user.id process_subtree $parent | ignore
let bob_subtree_availability = tg --token $bob.token availability $parent | from json
assert equal $bob_subtree_availability.subtree true "a process subtree grant should reveal the subtree availability"
