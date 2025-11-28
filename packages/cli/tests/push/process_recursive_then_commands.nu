use ../../test.nu *

# Recursively find all children commands and collect them into a list.
def collect_commands [process_id: string] {
	let process = tg get $process_id | from json
	mut commands = [$process.command]
	for child in $process.children {
		$commands = $commands | append (collect_commands $child.item)
	}
	$commands
}

export def test [path: string, ...args] {
	# Create a remote server.
	let remote_server = spawn -n remote

	# Create a local server.
	let local_server = spawn -n local

	# Add the remote.
	run tg remote put default $remote_server.url

	# Build the module.
	let output = run tg build -d $path | from json

	# Parse the process ID.
	let process_id = $output.process

	# Wait for the process to complete.
	tg wait $process_id

	let output = tg get $process_id | from json
	let command = $output.command
	let children = $output.children

	# Push the process without commands.
	run tg push "--recursive" ...$args $process_id

	# Confirm the process is on the remote and the same.
	let local_process = run tg get $process_id --pretty
	let remote_process = run tg --url $remote_server.url get $process_id --pretty
	assert equal $local_process $remote_process

	# Confirm output is present.
	if (($output.output | describe) | str starts-with 'record') {
		if $output.output.kind == "object" {
			run tg -u $remote_server.url get $output.output.value --pretty
		}
	}

	# Collect all commands from the process tree.
	let commands = collect_commands $process_id

	# For each command, confirm that they are NOT present on the remote.
	for command in $commands {
		let output = tg -u $remote_server.url get $command | complete
		failure $output
	}

	# Push the process again now with commands.
	run tg push "--recursive" "--commands" ...$args $process_id

	# Index on the remote.
	run tg -u $remote_server.url index

	# Confirm that all expected fields are present in the top-level metadata.
	let remote_metadata = run tg -u $remote_server.url metadata $process_id | from json
	assert ($remote_metadata.children? != null) "the metadata should contain the children field"
	assert ($remote_metadata.children_commands? != null) "the metadata should contain the children_commands field"
	assert ($remote_metadata.children_outputs? != null) "the metadata should contain the children_outputs field"
	assert ($remote_metadata.command? != null) "the metadata should contain the command field"
	assert ($remote_metadata.output? != null) "the metadata should contain the output field"

	# For each of the commands, confirm that they are present.
	for command in $commands {
		run tg -u $remote_server.url get $command --pretty
	}
}
