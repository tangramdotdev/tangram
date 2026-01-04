use ../../test.nu *

export def test [...args] {
	# Create a remote server.
	let remote = spawn --cloud -n remote

	# Create a local server.
	let local = spawn -n local

	# Create a source server.
	let source = spawn --cloud -n source

	# Create a module that spawns multiple child processes.
	let path = artifact {
		tangram.ts: '
			export default async () => {
				const results = [];
				for (let i = 0; i < 10; i++) {
					const result = await tg.build(makeFile, i);
					results.push(result);
				}
				return tg.directory(Object.fromEntries(results.map((r, i) => [`file_${i}`, r])));
			}

			export const makeFile = (i: number) => {
				return tg.file(`Content for file number ${i}. This is some text.`);
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
	let main_command_id = $process_data.command
	let main_output_id = $process_data.output.value
	let children = $process_data.children

	# Put the main process to the local server.
	tg -u $source.url get $process_id | tg -u $local.url put --id $process_id

	# Put the main command to the local server.
	tg -u $source.url get --bytes $main_command_id | tg -u $local.url put --bytes -k cmd

	# Get all the main command's descendants recursively and put to local.
	mut main_cmd_descendants = []
	mut to_visit = [$main_command_id]
	while ($to_visit | length) > 0 {
		let current = $to_visit | first
		$to_visit = ($to_visit | skip 1)
		let cmd_children = tg -u $source.url children $current | from json
		for child in $cmd_children {
			if $child not-in $main_cmd_descendants {
				$main_cmd_descendants = ($main_cmd_descendants | append $child)
				$to_visit = ($to_visit | append $child)
			}
		}
	}
	for child_id in $main_cmd_descendants {
		let kind = $child_id | str substring 0..<3
		tg -u $source.url get --bytes $child_id | tg -u $local.url put --bytes -k $kind
	}

	# Put the main output (directory) to the local server.
	tg -u $source.url get --bytes $main_output_id | tg -u $local.url put --bytes -k dir

	# Get all file children from the output directory.
	let output_files = tg -u $source.url children $main_output_id | from json

	# Put output files to local server.
	for fil_id in $output_files {
		tg -u $source.url get --bytes $fil_id | tg -u $local.url put --bytes -k fil
	}

	# Get blobs from the output files.
	let output_blobs = $output_files | each { |fil_id|
		tg -u $source.url children $fil_id | from json
	} | flatten | uniq

	# Put half of the output blobs to local, half to remote (leaf missing).
	let blob_count = $output_blobs | length
	let half_blobs = ($blob_count / 2 | math floor)
	for i in 0..<$blob_count {
		let blb_id = $output_blobs | get $i
		if $i < $half_blobs {
			tg -u $source.url get --bytes $blb_id | tg -u $local.url put --bytes -k blob
		} else {
			tg -u $source.url get --bytes $blb_id | tg -u $remote.url put --bytes -k blob
		}
	}

	# Process child processes - put some commands on remote (intermediate missing), some outputs on remote (leaf missing).
	let child_count = $children | length
	for i in 0..<$child_count {
		let child = $children | get $i
		let child_id = $child.item
		let child_data = tg -u $source.url get $child_id | from json
		let child_command_id = $child_data.command
		let child_output_id = $child_data.output.value

		# Put some child processes to local, some to remote (process root missing).
		if ($i mod 4) == 0 {
			# Put to remote only (root missing locally).
			tg -u $source.url get $child_id | tg -u $remote.url put --id $child_id
		} else {
			# Put to local.
			tg -u $source.url get $child_id | tg -u $local.url put --id $child_id
		}

		# Get all the child command's descendants recursively.
		mut child_cmd_descendants = []
		mut child_to_visit = [$child_command_id]
		while ($child_to_visit | length) > 0 {
			let current = $child_to_visit | first
			$child_to_visit = ($child_to_visit | skip 1)
			let cmd_children = tg -u $source.url children $current | from json
			for child in $cmd_children {
				if $child not-in $child_cmd_descendants {
					$child_cmd_descendants = ($child_cmd_descendants | append $child)
					$child_to_visit = ($child_to_visit | append $child)
				}
			}
		}

		# Alternate between missing commands and missing outputs.
		if ($i mod 3) == 0 {
			# Put command to remote (intermediate missing), output to local.
			tg -u $source.url get --bytes $child_command_id | tg -u $remote.url put --bytes -k cmd
			for desc_id in $child_cmd_descendants {
				let kind = $desc_id | str substring 0..<3
				tg -u $source.url get --bytes $desc_id | tg -u $remote.url put --bytes -k $kind
			}
			tg -u $source.url get --bytes $child_output_id | tg -u $local.url put --bytes -k fil

			# Put the blob for this output to local.
			let child_blob_ids = tg -u $source.url children $child_output_id | from json
			for blb_id in $child_blob_ids {
				tg -u $source.url get --bytes $blb_id | tg -u $local.url put --bytes -k blob
			}
		} else if ($i mod 3) == 1 {
			# Put command to local, output to remote (leaf missing).
			tg -u $source.url get --bytes $child_command_id | tg -u $local.url put --bytes -k cmd
			for desc_id in $child_cmd_descendants {
				let kind = $desc_id | str substring 0..<3
				tg -u $source.url get --bytes $desc_id | tg -u $local.url put --bytes -k $kind
			}
			tg -u $source.url get --bytes $child_output_id | tg -u $remote.url put --bytes -k fil

			# Put the blob for this output to remote.
			let child_blob_ids = tg -u $source.url children $child_output_id | from json
			for blb_id in $child_blob_ids {
				tg -u $source.url get --bytes $blb_id | tg -u $remote.url put --bytes -k blob
			}
		} else {
			# Put both command and output to local.
			tg -u $source.url get --bytes $child_command_id | tg -u $local.url put --bytes -k cmd
			for desc_id in $child_cmd_descendants {
				let kind = $desc_id | str substring 0..<3
				tg -u $source.url get --bytes $desc_id | tg -u $local.url put --bytes -k $kind
			}
			tg -u $source.url get --bytes $child_output_id | tg -u $local.url put --bytes -k fil

			# Put the blob for this output to local.
			let child_blob_ids = tg -u $source.url children $child_output_id | from json
			for blb_id in $child_blob_ids {
				tg -u $source.url get --bytes $blb_id | tg -u $local.url put --bytes -k blob
			}
		}
	}

	# Index.
	tg -u $local.url index
	tg -u $remote.url index

	# Add the remote to the local server.
	tg -u $local.url remote put default $remote.url

	# Push the process with recursive and commands flags.
	tg -u $local.url push $process_id ...$args --recursive --commands

	# Index on both servers.
	tg -u $source.url index
	tg -u $remote.url index

	# Confirm metadata matches.
	let source_metadata = tg -u $source.url process metadata $process_id --pretty
	let remote_metadata = tg -u $remote.url process metadata $process_id --pretty
	assert equal $source_metadata $remote_metadata
}
