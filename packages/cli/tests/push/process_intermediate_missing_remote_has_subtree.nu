use ../../test.nu *

def test [...args] {
	# Create a remote server.
	let remote = spawn --cloud -n remote

	# Create a local server.
	let local = spawn -n local

	# Create a source server.
	let source = spawn -n source

	# Create a module that spawns a chain of 4 child processes: A -> B -> C -> D.
	# A calls B, B calls C, C calls D, D returns a file.
	let path = artifact {
		tangram.ts: '
			export default async () => {
				// Process A - calls B.
				return await tg.build(processB);
			}

			export const processB = async () => {
				// Process B - calls C.
				return await tg.build(processC);
			}

			export const processC = async () => {
				// Process C - calls D.
				return await tg.build(processD);
			}

			export const processD = async () => {
				// Process D - returns a file.
				return tg.file("Hello from process D!");
			}
		'
	}

	# Build the module on the dummy server.
	let process_a_id = tg -u $source.url build -d $path | str trim

	# Wait for the process to finish.
	tg -u $source.url wait $process_a_id
	tg -u $source.url index

	# Get process A's data.
	let process_a_data = tg -u $source.url get $process_a_id | from json
	let command_a_id = $process_a_data.command
	let output_a_id = $process_a_data.output.value
	let log_a_id = $process_a_data.log
	let children_a = $process_a_data.children

	# Get process B (first child of A).
	let process_b_id = $children_a | get 0 | get item
	let process_b_data = tg -u $source.url get $process_b_id | from json
	let command_b_id = $process_b_data.command
	let output_b_id = $process_b_data.output.value
	let log_b_id = $process_b_data.log
	let children_b = $process_b_data.children

	# Get process C (first child of B).
	let process_c_id = $children_b | get 0 | get item
	let process_c_data = tg -u $source.url get $process_c_id | from json
	let command_c_id = $process_c_data.command
	let output_c_id = $process_c_data.output.value
	let log_c_id = $process_c_data.log
	let children_c = $process_c_data.children

	# Get process D (first child of C).
	let process_d_id = $children_c | get 0 | get item
	let process_d_data = tg -u $source.url get $process_d_id | from json
	let command_d_id = $process_d_data.command
	let output_d_id = $process_d_data.output.value
	let log_d_id = $process_d_data.log

	# Helper function to get all command descendants recursively.
	def get_command_descendants [server_url: string, cmd_id: string] {
		mut descendants = []
		mut to_visit = [$cmd_id]
		while ($to_visit | length) > 0 {
			let current = $to_visit | first
			$to_visit = ($to_visit | skip 1)
			let children = tg -u $server_url children $current | from json
			for child in $children {
				if $child not-in $descendants {
					$descendants = ($descendants | append $child)
					$to_visit = ($to_visit | append $child)
				}
			}
		}
		$descendants
	}

	# Get output blob.
	let output_d_children = tg -u $source.url children $output_d_id | from json
	let blob_d_id = $output_d_children | get 0

	# Put process A to the local server (local has A).
	tg -u $source.url get $process_a_id | tg -u $local.url put --id $process_a_id

	# Put command A and its descendants to the local server.
	tg -u $source.url get --bytes $command_a_id | tg -u $local.url put --bytes -k cmd
	let command_a_descendants = get_command_descendants $source.url $command_a_id
	for desc_id in $command_a_descendants {
		let kind = $desc_id | str substring 0..<3
		tg -u $source.url get --bytes $desc_id | tg -u $local.url put --bytes -k $kind
	}

	# Put output A to the local server (which is the same as output B, C, D since they pass through).
	tg -u $source.url get --bytes $output_a_id | tg -u $local.url put --bytes -k fil
	tg -u $source.url get --bytes $blob_d_id | tg -u $local.url put --bytes -k blob

	# Put log A to the local server.
	tg -u $source.url get --bytes $log_a_id | tg -u $local.url put --bytes -k blob

	# Local does NOT have process B (the intermediate process is missing locally).

	# Put process B, C, and D to the remote server (remote has everything from B downward).
	tg -u $source.url get $process_b_id | tg -u $remote.url put --id $process_b_id

	# Put command B and its descendants to the remote server.
	tg -u $source.url get --bytes $command_b_id | tg -u $remote.url put --bytes -k cmd
	let command_b_descendants = get_command_descendants $source.url $command_b_id
	for desc_id in $command_b_descendants {
		let kind = $desc_id | str substring 0..<3
		tg -u $source.url get --bytes $desc_id | tg -u $remote.url put --bytes -k $kind
	}

	# Put output B to the remote server.
	tg -u $source.url get --bytes $output_b_id | tg -u $remote.url put --bytes -k fil

	# Put process C and D to the remote server.
	tg -u $source.url get $process_c_id | tg -u $remote.url put --id $process_c_id
	tg -u $source.url get $process_d_id | tg -u $remote.url put --id $process_d_id

	# Put command C, D and their descendants to the remote server.
	tg -u $source.url get --bytes $command_c_id | tg -u $remote.url put --bytes -k cmd
	let command_c_descendants = get_command_descendants $source.url $command_c_id
	for desc_id in $command_c_descendants {
		let kind = $desc_id | str substring 0..<3
		tg -u $source.url get --bytes $desc_id | tg -u $remote.url put --bytes -k $kind
	}

	tg -u $source.url get --bytes $command_d_id | tg -u $remote.url put --bytes -k cmd
	let command_d_descendants = get_command_descendants $source.url $command_d_id
	for desc_id in $command_d_descendants {
		let kind = $desc_id | str substring 0..<3
		tg -u $source.url get --bytes $desc_id | tg -u $remote.url put --bytes -k $kind
	}

	# Put outputs C and D to the remote server.
	tg -u $source.url get --bytes $output_c_id | tg -u $remote.url put --bytes -k fil
	tg -u $source.url get --bytes $output_d_id | tg -u $remote.url put --bytes -k fil
	tg -u $source.url get --bytes $blob_d_id | tg -u $remote.url put --bytes -k blob

	# Put logs B, C, D to the remote server.
	tg -u $source.url get --bytes $log_b_id | tg -u $remote.url put --bytes -k blob
	tg -u $source.url get --bytes $log_c_id | tg -u $remote.url put --bytes -k blob
	tg -u $source.url get --bytes $log_d_id | tg -u $remote.url put --bytes -k blob

	# Confirm process B is not on the local server.
	let output = tg -u $local.url get $process_b_id | complete
	failure $output

	# Confirm process A is not on the remote server.
	let output = tg -u $remote.url get $process_a_id | complete
	failure $output

	# Index.
	tg -u $local.url index
	tg -u $remote.url index

	# Add the remote to the local server.
	tg -u $local.url remote put default $remote.url

	# Push the process with recursive and command flags.
	tg -u $local.url push $process_a_id --recursive --command --log ...$args

	# Index.
	tg -u $source.url index
	tg -u $remote.url index

	# Confirm process A is on the remote and the same.
	let source_process_a = tg -u $source.url get $process_a_id --pretty
	let remote_process_a = tg -u $remote.url get $process_a_id --pretty
	assert equal $source_process_a $remote_process_a

	# Confirm process B is on the remote and the same.
	let source_process_b = tg -u $source.url get $process_b_id --pretty
	let remote_process_b = tg -u $remote.url get $process_b_id --pretty
	assert equal $source_process_b $remote_process_b

	# Confirm process C is on the remote and the same.
	let source_process_c = tg -u $source.url get $process_c_id --pretty
	let remote_process_c = tg -u $remote.url get $process_c_id --pretty
	assert equal $source_process_c $remote_process_c

	# Confirm process D is on the remote and the same.
	let source_process_d = tg -u $source.url get $process_d_id --pretty
	let remote_process_d = tg -u $remote.url get $process_d_id --pretty
	assert equal $source_process_d $remote_process_d

	# Confirm all commands are on the remote.
	tg -u $remote.url get $command_a_id --pretty
	tg -u $remote.url get $command_b_id --pretty
	tg -u $remote.url get $command_c_id --pretty
	tg -u $remote.url get $command_d_id --pretty

	# Confirm all outputs are on the remote.
	tg -u $remote.url get $output_a_id --pretty
	tg -u $remote.url get $output_d_id --pretty
}

test "--eager"
test "--lazy"
