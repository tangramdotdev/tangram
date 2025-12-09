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
	tg remote put default $remote_server.url

	# Build the module.
	let output = tg build -d $path | from json

	# Parse the process ID.
	let process_id = $output.process

	# Wait for the process to finish.
	tg wait $process_id

	let output = tg get $process_id | from json
	let command = $output.command
	let children = $output.children

	# Push the process without commands.
	tg push "--recursive" ...$args $process_id

	# Confirm the process is on the remote and the same.
	let local_process = tg get $process_id --pretty
	let remote_process = tg --url $remote_server.url get $process_id --pretty
	assert equal $local_process $remote_process

	# Confirm output is present.
	if (($output.output | describe) | str starts-with 'record') {
		if $output.output.kind == "object" {
			tg -u $remote_server.url get $output.output.value --pretty
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
	tg push "--recursive" "--commands" ...$args $process_id

	# Index on the remote.
	tg -u $remote_server.url index

	# Confirm that all expected fields are present in the top-level metadata.
	let remote_metadata = tg -u $remote_server.url metadata $process_id | from json
	assert ($remote_metadata.subtree?.process_count? != null) "the metadata should contain the subtree.process_count field"
	assert ($remote_metadata.subtree?.command? != null) "the metadata should contain the subtree.command field"
	assert ($remote_metadata.subtree?.output? != null) "the metadata should contain the subtree.output field"
	assert ($remote_metadata.node?.command? != null) "the metadata should contain the node.command field"
	assert ($remote_metadata.node?.output? != null) "the metadata should contain the node.output field"

	# For each of the commands, confirm that they are present.
	for command in $commands {
		tg -u $remote_server.url get $command --pretty
	}
}
