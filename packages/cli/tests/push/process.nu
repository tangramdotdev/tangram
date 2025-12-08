use ../../test.nu *

export def test [path: string, ...args] {
	# Create a remote server.
	let remote_server = spawn -n remote

	# Create a local server.
	let local_server = spawn -n local

	# Add the remote.
	tg remote put default $remote_server.url | complete

	# Build the module.
	let output = run tg build -d $path | from json

	# Parse the process ID.
	let process_id = $output.process

	# Wait for the process to finish.
	tg wait $process_id

	let output = tg get $process_id | from json
	let command = $output.command
	let children = $output.children

	# Push the process.
	run tg push ...$args $process_id

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

	# Confirm commands are present if --commmands.
	if "--commands" in $args {
		run tg -u $remote_server.url get $output.command --pretty
	}

	# Confirm children are present if --recursive.
	if "--recursive" in $args {
		for child in $children {
			run tg -u $remote_server.url get $child.item
		}
	}

	# Confirm children commands are present if --recursive and --commands.
	if "--commands" in $args and "--recursive" in $args {
		for child in $children {
			let output = tg -u $remote_server.url get $child.item | from json
			run tg -u $remote_server.url get $output.command --pretty
		}
	}

	# Confirm children output is present if --recursive.
	if "--recursive" in $args {
		for child in $children {
			let output = tg get $child.item | from json
			if (($output.output | describe) | str starts-with 'record') {
				if $output.output.kind == "object" {
					run tg -u $remote_server.url get $output.output.value --pretty
				}
			}
		}
	}

}
