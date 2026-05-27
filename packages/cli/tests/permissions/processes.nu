use ../../test.nu *

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
tg user login bob
let bob = current_token

let alice_process = "pcs_00081061050r3gg28a1c60t3gf20"
let process_data = {
	children: [],
	command: "cmd_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0",
	created_at: 0,
	host: "test",
	sandbox: "sbx_00041061050r3gg28a1c60t3gf20",
	status: "created",
}

tg --token $alice process put $alice_process ($process_data | merge { cacheable: true } | to json)
let output = tg --token $alice signal --lease invalid $alice_process | complete
failure $output "Alice should not be able to signal a cacheable process."
assert ($output.stderr | str contains "cannot signal cacheable processes") "The error should mention cacheable processes."

tg --token $alice process put $alice_process ($process_data | to json)
tg --token $alice process get $alice_process
tg --token $alice process get $alice_process --metadata
tg --token $alice process children $alice_process
tg --token $alice index
tg --token $alice process metadata $alice_process

let output = tg --token $bob process get $alice_process | complete
failure $output "Bob should not be able to get Alice's process without a grant."
assert ($output.stderr | str contains "failed to find the process") "The error should match the missing process behavior."

let output = tg --token $bob process get $alice_process --metadata | complete
failure $output "Bob should not be able to get Alice's process and metadata without a grant."
assert ($output.stderr | str contains "failed to find the process") "The error should match the missing process behavior."

let output = tg --token $bob process children $alice_process | complete
failure $output "Bob should not be able to get Alice's process children without a grant."

let output = tg --token $bob process metadata $alice_process | complete
failure $output "Bob should not be able to get Alice's process metadata without a grant."
assert ($output.stderr | str contains "failed to find the process") "The error should match the missing process behavior."

let alice_processes = tg --token $alice process list | from json
assert ($alice_processes | any {|process| $process.id == $alice_process }) "Alice should be able to list processes she created."
let bob_processes = tg --token $bob process list | from json
assert (not ($bob_processes | any {|process| $process.id == $alice_process })) "Bob should not list Alice's process."
