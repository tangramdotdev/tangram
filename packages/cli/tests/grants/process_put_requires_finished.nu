use ../../test.nu *

# The public process put API rejects unfinished process data.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose alice | from json

let process = "pcs_00081061050r3gg28a1c60t3gf20"
let process_data = {
	children: [],
	command: "cmd_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0",
	created_at: 0,
	host: "test",
	sandbox: "sbx_00041061050r3gg28a1c60t3gf20",
	status: "started",
}

let output = tg --token $alice.token process put $process ($process_data | to json) | complete
failure $output "putting unfinished process data through the public API should fail."
snapshot --normalize-ids $output.stderr '
	error an error occurred
	-> failed to put the process
	   id = pcs_0000000000000000000000000000
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to put the process
	   id = pcs_0000000000000000000000000000
	-> expected a finished process

'
