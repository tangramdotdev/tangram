use ../test.nu *

# Spawn a remote and local server.
let remote = spawn --cloud -n remote
let local = spawn -n local

# Add the remote to the local server.
let output = tg remote put default $remote.url | complete
success $output

# Build a local process.
let push_path = artifact {
	tangram.ts: '
		export default () => {
			return tg.file("pushed")
		}
	'
}
let push_process_id = tg build -d $push_path | str trim
tg wait $push_process_id
let push_process = tg get $push_process_id | from json
let push_command_id = $push_process.command
let push_output_id = $push_process.output.value

# Push and verify the items are directly gettable on the remote before indexing.
let output = tg push --commands $push_process_id | complete
success $output
let local_process = tg get $push_process_id --pretty
let remote_process = tg --url $remote.url get $push_process_id --pretty
assert equal $local_process $remote_process
let output = tg --url $remote.url get $push_command_id --pretty | complete
success $output
let output = tg --url $remote.url get $push_output_id --pretty | complete
success $output

# Build a remote process.
let pull_path = artifact {
	tangram.ts: '
		export default () => {
			return tg.file("pulled")
		}
	'
}
let pull_process_id = tg --url $remote.url build -d $pull_path | str trim
tg --url $remote.url wait $pull_process_id
let pull_process = tg --url $remote.url get $pull_process_id | from json
let pull_command_id = $pull_process.command
let pull_output_id = $pull_process.output.value

# Pull and verify the items are directly gettable locally before indexing.
let output = tg pull --commands $pull_process_id | complete
success $output
let remote_process = tg --url $remote.url get $pull_process_id --pretty
let local_process = tg get $pull_process_id --pretty
assert equal $remote_process $local_process
let output = tg get $pull_command_id --pretty | complete
success $output
let output = tg get $pull_output_id --pretty | complete
success $output
