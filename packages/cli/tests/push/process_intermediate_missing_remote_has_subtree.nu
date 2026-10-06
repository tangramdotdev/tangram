use ../lib/test.nu *
use ../lib/command.nu

# Recursively pushing a chain of child processes where an intermediate process is missing locally but the remote already holds that process subtree completes and makes every process, command, and output present on the remote, under both eager and lazy push.

def test [...args] {
	# Create a remote server.
	let remote = server spawn --cloud --name remote

	# Create a local server.
	let local = server spawn --name local

	# Create a source server.
	let local_source = server spawn --name local-source

	# Configure destinations for copying complete log blobs.
	tg --url $local_source.url remote put fixture-local $local.url
	tg --url $local_source.url remote put fixture-remote $remote.url

	# Create a module that spawns a chain of 4 child processes: A -> B -> C -> D.
	# A calls B, B calls C, C calls D, D returns a file.
	let path = artifact {
		tangram.ts: '
			export default async function () {
				// Process A - calls B.
				return await tg.build(processB);
			}

			export async function processB() {
				// Process B - calls C.
				return await tg.build(processC);
			}

			export async function processC() {
				// Process C - calls D.
				return await tg.build(processD);
			}

			export async function processD() {
				// Process D - returns a file.
				return tg.file("Hello from process D!");
			}
		'
	}

	# Build the module on the dummy server.
	let process_a_id = tg --url $local_source.url build --no-tokens --detach $path | referent node

	# Wait for the process to finish.
	tg --url $local_source.url wait $process_a_id

	# Get process A's data.
	tg --url $local_source.url index
	let process_a_data = tg --url $local_source.url get $process_a_id | from json
	let module_a_id = (command module-input $process_a_data.command)
	let output_a_id = $process_a_data.output.value
	let log_a_id = $process_a_data.log
	let children_a = $process_a_data.children

	# Get process B (first child of A).
	let process_b_id = $children_a | get 0 | get process | split row '?' | first
	tg --url $local_source.url index
	let process_b_data = tg --url $local_source.url get $process_b_id | from json
	let module_b_id = (command module-input $process_b_data.command)
	let output_b_id = $process_b_data.output.value
	let log_b_id = $process_b_data.log
	let children_b = $process_b_data.children

	# Get process C (first child of B).
	let process_c_id = $children_b | get 0 | get process | split row '?' | first
	tg --url $local_source.url index
	let process_c_data = tg --url $local_source.url get $process_c_id | from json
	let module_c_id = (command module-input $process_c_data.command)
	let output_c_id = $process_c_data.output.value
	let log_c_id = $process_c_data.log
	let children_c = $process_c_data.children

	# Get process D (first child of C).
	let process_d_id = $children_c | get 0 | get process | split row '?' | first
	tg --url $local_source.url index
	let process_d_data = tg --url $local_source.url get $process_d_id | from json
	let module_d_id = (command module-input $process_d_data.command)
	let output_d_id = $process_d_data.output.value
	let log_d_id = $process_d_data.log

	# Helper function to get all module descendants recursively.
	def get_module_descendants [server_url: string, module_id: string] {
		mut descendants = []
		mut to_visit = [$module_id]
		while ($to_visit | length) > 0 {
			let current = $to_visit | first
			$to_visit = ($to_visit | skip 1)
			let children = tg --url $server_url children $current | from json
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
	let output_d_children = tg --url $local_source.url children $output_d_id | from json
	let blob_d_id = $output_d_children | get 0

	# Put process A to the local server (local has A).
	tg --url $local_source.url get $process_a_id | tg --url $local.url put --no-tokens --id $process_a_id | referent node

	# Put module A and its descendants to the local server.
	tg --url $local_source.url get --bytes $module_a_id | tg --url $local.url put --no-tokens --bytes --kind fil | referent node
	let module_a_descendants = get_module_descendants $local_source.url $module_a_id
	for desc_id in $module_a_descendants {
		let kind = $desc_id | str substring 0..<3
		tg --url $local_source.url get --bytes $desc_id | tg --url $local.url put --no-tokens --bytes --kind $kind | referent node
	}

	# Put output A to the local server (which is the same as output B, C, D since they pass through).
	tg --url $local_source.url get --bytes $output_a_id | tg --url $local.url put --no-tokens --bytes --kind fil | referent node
	tg --url $local_source.url get --bytes $blob_d_id | tg --url $local.url put --no-tokens --bytes --kind blob | referent node

	# Put log A to the local server.
	tg --url $local_source.url push $log_a_id --remote=fixture-local --no-tokens

	# Local does NOT have process B (the intermediate process is missing locally).

	# Put process B, C, and D to the remote server (remote has everything from B downward).
	tg --url $local_source.url get $process_b_id | tg --url $remote.url put --no-tokens --id $process_b_id | referent node

	# Put module B and its descendants to the remote server.
	tg --url $local_source.url get --bytes $module_b_id | tg --url $remote.url put --no-tokens --bytes --kind fil | referent node
	let module_b_descendants = get_module_descendants $local_source.url $module_b_id
	for desc_id in $module_b_descendants {
		let kind = $desc_id | str substring 0..<3
		tg --url $local_source.url get --bytes $desc_id | tg --url $remote.url put --no-tokens --bytes --kind $kind | referent node
	}

	# Put output B to the remote server.
	tg --url $local_source.url get --bytes $output_b_id | tg --url $remote.url put --no-tokens --bytes --kind fil | referent node

	# Put process C and D to the remote server.
	tg --url $local_source.url get $process_c_id | tg --url $remote.url put --no-tokens --id $process_c_id | referent node
	tg --url $local_source.url get $process_d_id | tg --url $remote.url put --no-tokens --id $process_d_id | referent node

	# Put module C, D and their descendants to the remote server.
	tg --url $local_source.url get --bytes $module_c_id | tg --url $remote.url put --no-tokens --bytes --kind fil | referent node
	let module_c_descendants = get_module_descendants $local_source.url $module_c_id
	for desc_id in $module_c_descendants {
		let kind = $desc_id | str substring 0..<3
		tg --url $local_source.url get --bytes $desc_id | tg --url $remote.url put --no-tokens --bytes --kind $kind | referent node
	}

	tg --url $local_source.url get --bytes $module_d_id | tg --url $remote.url put --no-tokens --bytes --kind fil | referent node
	let module_d_descendants = get_module_descendants $local_source.url $module_d_id
	for desc_id in $module_d_descendants {
		let kind = $desc_id | str substring 0..<3
		tg --url $local_source.url get --bytes $desc_id | tg --url $remote.url put --no-tokens --bytes --kind $kind | referent node
	}

	# Put outputs C and D to the remote server.
	tg --url $local_source.url get --bytes $output_c_id | tg --url $remote.url put --no-tokens --bytes --kind fil | referent node
	tg --url $local_source.url get --bytes $output_d_id | tg --url $remote.url put --no-tokens --bytes --kind fil | referent node
	tg --url $local_source.url get --bytes $blob_d_id | tg --url $remote.url put --no-tokens --bytes --kind blob | referent node

	# Put logs B, C, D to the remote server.
	tg --url $local_source.url push $log_b_id --remote=fixture-remote --no-tokens
	tg --url $local_source.url push $log_c_id --remote=fixture-remote --no-tokens
	tg --url $local_source.url push $log_d_id --remote=fixture-remote --no-tokens

	# Confirm process B is not on the local server.
	let output = tg --url $local.url get $process_b_id | complete
	failure $output

	# Confirm process A is not on the remote server.
	let output = tg --url $remote.url get $process_a_id | complete
	failure $output

	# Index.
	tg --url $local.url index
	tg --url $remote.url index

	# Add the remote to the local server.
	tg --url $local.url remote put default $remote.url

	# Push the process with recursive and command flags.
	tg --url $local.url push $process_a_id --process-children --process-command-objects --process-log-objects ...$args

	# Index.
	tg --url $local_source.url index
	tg --url $remote.url index

	# Confirm process A is on the remote and the same.
	let source_process_a = tg --url $local_source.url get $process_a_id --no-tokens --pretty
	let remote_process_a = tg --url $remote.url get $process_a_id --no-tokens --pretty
	assert equal $source_process_a $remote_process_a

	# Confirm process B is on the remote and the same.
	let source_process_b = tg --url $local_source.url get $process_b_id --no-tokens --pretty
	let remote_process_b = tg --url $remote.url get $process_b_id --no-tokens --pretty
	assert equal $source_process_b $remote_process_b

	# Confirm process C is on the remote and the same.
	let source_process_c = tg --url $local_source.url get $process_c_id --no-tokens --pretty
	let remote_process_c = tg --url $remote.url get $process_c_id --no-tokens --pretty
	assert equal $source_process_c $remote_process_c

	# Confirm process D is on the remote and the same.
	let source_process_d = tg --url $local_source.url get $process_d_id --no-tokens --pretty
	let remote_process_d = tg --url $remote.url get $process_d_id --no-tokens --pretty
	assert equal $source_process_d $remote_process_d

	# Confirm all command inputs are on the remote.
	tg --url $remote.url get $module_a_id --no-tokens --pretty
	tg --url $remote.url get $module_b_id --no-tokens --pretty
	tg --url $remote.url get $module_c_id --no-tokens --pretty
	tg --url $remote.url get $module_d_id --no-tokens --pretty

	# Confirm all outputs are on the remote.
	tg --url $remote.url get $output_a_id --no-tokens --pretty
	tg --url $remote.url get $output_d_id --no-tokens --pretty
}

test "--eager"
test "--lazy"
