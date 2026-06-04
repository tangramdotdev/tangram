use ../../test.nu *

# Pushing a process whose command is missing locally but present on the remote completes and yields matching processes and metadata, under both eager and lazy push.

def test [...args] {
	# Create a remote server.
	let remote = spawn --cloud --name remote

	# Create a local server.
	let local = spawn --name local

	# Create a source server.
	let source = spawn --name source

	# Add the remote to the local server.
	tg remote put default $remote.url

	let path = artifact {
		tangram.ts: '
			export default () => {
				return tg.file("Hello, World!")
			}
		'
	}

	# Build the module.
	let process_id = tg --url $source.url build --detach $path | str trim

	# Wait for the process to finish.
	tg --url $source.url wait $process_id
	tg --url $source.url index

	# Get the process data.
	let process_data = tg --url $source.url get $process_id | from json
	let command_id = $process_data.command
	let output_id = $process_data.output.value

	# Get the output's children (the blob).
	let output_children = tg --url $source.url children $output_id | from json
	let blb_id = $output_children | get 0

	# Get all the command's descendants recursively by manually traversing the tree.
	mut all_descendants = []
	mut to_visit = [$command_id]
	while ($to_visit | length) > 0 {
		let current = $to_visit | first
		$to_visit = ($to_visit | skip 1)
		let children = tg --url $source.url children $current | from json
		for child in $children {
			if $child not-in $all_descendants {
				$all_descendants = ($all_descendants | append $child)
				$to_visit = ($to_visit | append $child)
			}
		}
	}

	# Put the process to the local server.
	tg --url $source.url get $process_id | tg --url $local.url put --id $process_id

	# Put the command to the remote server (intermediate missing locally).
	tg --url $source.url get --bytes $command_id | tg --url $remote.url put --bytes --kind cmd

	# Put the command's descendants to the remote server.
	for child_id in $all_descendants {
		let kind = $child_id | str substring 0..<3
		tg --url $source.url get --bytes $child_id | tg --url $remote.url put --bytes --kind $kind
	}

	# Put the output to the local server.
	tg --url $source.url get --bytes $output_id | tg --url $local.url put --bytes --kind fil

	# Put the output's blob to the local server.
	tg --url $source.url get --bytes $blb_id | tg --url $local.url put --bytes --kind blob

	# Put the log to the local server.
	let log_id = tg --url $source.url get $process_id | from json | get log
	tg --url $source.url get --bytes $log_id | tg --url $local.url put --bytes --kind blob

	# Confirm the command is not on the local server.
	let output = tg --url $local.url get $command_id | complete
	failure $output

	# Confirm the output is not on the remote server.
	let output = tg --url $remote.url get $output_id | complete
	failure $output

	# Index.
	tg --url $local.url index
	tg --url $remote.url index

	# Add the remote to the local server.
	tg --url $local.url remote put default $remote.url

	# Push the process.
	tg --url $local.url push $process_id --command --log ...$args

	# Confirm the process is on the source and remote.
	let source_process = tg --url $source.url get $process_id --pretty
	let remote_process = tg --url $remote.url get $process_id --pretty
	assert equal $source_process $remote_process

	# Index.
	tg --url $source.url index
	tg --url $remote.url index

	# Confirm metadata matches.
	let source_metadata = tg --url $source.url process metadata $process_id --pretty
	let remote_metadata = tg --url $remote.url process metadata $process_id --pretty
	assert equal $source_metadata $remote_metadata
}

test "--eager"
test "--lazy"
