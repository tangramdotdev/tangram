use ../lib/test.nu *
use ../lib/command.nu

# Pushing a process whose output blob is missing locally but present on the remote completes and yields matching processes and metadata, under both eager and lazy push.

def test [...args] {
	# Create a remote server.
	let remote = server spawn --cloud --name remote

	# Create a local server.
	let local = server spawn --name local

	# Create a source server.
	let local_source = server spawn --name local-source

	let path = artifact {
		tangram.ts: '
			export default function () {
				return tg.file("Hello, World!")
			}
		'
	}

	# Build the module.
	let process_id = tg --url $local_source.url build --no-tokens --detach $path | referent node

	# Wait for the process to finish.
	tg --url $local_source.url wait $process_id
	tg --url $local_source.url index

	# Get the process data.
	let process_data = tg --url $local_source.url get $process_id | from json
	let module_id = (command module-input $process_data.command)
	let output_id = $process_data.output.value

	# Get the output's children (the blob).
	let output_children = tg --url $local_source.url children $output_id | from json
	let blb_id = $output_children | get 0

	# Get all the module's descendants recursively by manually traversing the tree.
	mut all_descendants = []
	mut to_visit = [$module_id]
	while ($to_visit | length) > 0 {
		let current = $to_visit | first
		$to_visit = ($to_visit | skip 1)
		let children = tg --url $local_source.url children $current | from json
		for child in $children {
			if $child not-in $all_descendants {
				$all_descendants = ($all_descendants | append $child)
				$to_visit = ($to_visit | append $child)
			}
		}
	}

	# Put the process to the local server.
	tg --url $local_source.url get $process_id | tg --url $local.url put --no-tokens --id $process_id | referent node

	# Put the module to the local server.
	tg --url $local_source.url get --bytes $module_id | tg --url $local.url put --no-tokens --bytes --kind fil | referent node

	# Put the module's descendants to the local server.
	for child_id in $all_descendants {
		let kind = $child_id | str substring 0..<3
		tg --url $local_source.url get --bytes $child_id | tg --url $local.url put --no-tokens --bytes --kind $kind | referent node
	}

	# Put the output to the local server.
	tg --url $local_source.url get --bytes $output_id | tg --url $local.url put --no-tokens --bytes --kind fil | referent node

	# Put the output's blob to the remote server.
	tg --url $local_source.url get --bytes $blb_id | tg --url $remote.url put --no-tokens --bytes --kind blob | referent node

	# Confirm the blob is not on the local server.
	let output = tg --url $local.url get $blb_id --blobs | complete
	failure $output

	# Put the log to the remote server.
	let log_id = tg --url $local_source.url get $process_id | from json | get log
	tg --url $local_source.url get --bytes $log_id | tg --url $remote.url put --no-tokens --bytes --kind blob | referent node

	# Index.
	tg --url $local.url index
	tg --url $remote.url index

	# Add the remote to the local server.
	tg --url $local.url remote put default $remote.url

	# Push the process.
	tg --url $local.url push $process_id --process-command-objects --process-log-objects ...$args

	# Confirm the process is on the remote and the same.
	let source_process = tg --url $local_source.url get $process_id --no-tokens --pretty
	let remote_process = tg --url $remote.url get $process_id --no-tokens --pretty
	assert equal $source_process $remote_process

	# Index.
	tg --url $local_source.url index
	tg --url $remote.url index

	# Confirm metadata matches.
	let source_metadata = tg --url $local_source.url process metadata $process_id --pretty
	let remote_metadata = tg --url $remote.url process metadata $process_id --pretty
	assert equal $source_metadata $remote_metadata
}

test "--eager"
test "--lazy"
