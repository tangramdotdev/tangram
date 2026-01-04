use ../../test.nu *

export def test [...args] {
	# Create a remote server.
	let remote = spawn -n remote

	# Create a local server.
	let local = spawn -n local

	# Create a source server.
	let source = spawn -n source

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
	let output = tg -u $source.url build -d $path | from json
	let process_id = $output.process

	# Wait for the process to finish.
	tg -u $source.url wait $process_id

	# Get the process data.
	let process_data = tg -u $source.url get $process_id | from json
	let command_id = $process_data.command
	let output_id = $process_data.output.value

	# Get the output's children (the blob).
	let output_children = tg -u $source.url children $output_id | from json
	let blb_id = $output_children | get 0

	# Get all the command's descendants recursively by manually traversing the tree.
	mut all_descendants = []
	mut to_visit = [$command_id]
	while ($to_visit | length) > 0 {
		let current = $to_visit | first
		$to_visit = ($to_visit | skip 1)
		let children = tg -u $source.url children $current | from json
		for child in $children {
			if $child not-in $all_descendants {
				$all_descendants = ($all_descendants | append $child)
				$to_visit = ($to_visit | append $child)
			}
		}
	}

	# Put the process to the remote server only (root missing locally).
	tg -u $source.url get $process_id | tg -u $remote.url put --id $process_id

	# Put the command to the local server.
	tg -u $source.url get --bytes $command_id | tg -u $local.url put --bytes -k cmd

	# Put the command's descendants to the local server.
	for child_id in $all_descendants {
		let kind = $child_id | str substring 0..<3
		tg -u $source.url get --bytes $child_id | tg -u $local.url put --bytes -k $kind
	}

	# Put the output (file) to the local server.
	tg -u $source.url get --bytes $output_id | tg -u $local.url put --bytes -k fil

	# Put the blob to the local server.
	tg -u $source.url get --bytes $blb_id | tg -u $local.url put --bytes -k blob

	# Confirm the process is not on the local server.
	let output = tg -u $local.url get $process_id | complete
	failure $output

	# Index.
	tg -u $local.url index
	tg -u $remote.url index

	# Add the remote to the local server.
	tg -u $local.url remote put default $remote.url

	# Push the process.
	tg -u $local.url push $process_id --commands ...$args

	# Confirm the process is on the remote and the same.
	let source_process = tg -u $source.url get $process_id --pretty
	let remote_process = tg -u $remote.url get $process_id --pretty
	assert equal $source_process $remote_process

	# Index.
	tg -u $source.url index
	tg -u $remote.url index

	# Confirm metadata matches.
	let source_metadata = tg -u $source.url process metadata $process_id --pretty
	let remote_metadata = tg -u $remote.url process metadata $process_id --pretty
	assert equal $source_metadata $remote_metadata
}
