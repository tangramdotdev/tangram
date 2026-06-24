use ../../test.nu *

# Re-pushing a process the remote already has transfers no process, while pushing with --force re-transfers the process.

# Create a remote server.
let remote = spawn --cloud --name remote

# Create a local server.
let local = spawn --name local

# Add the remote to the local server.
let output = tg remote put default $remote.url | complete
success $output

# Create a simple process.
let path = artifact {
	tangram.ts: '
		export default function () {
			return 5
		}
	'
}
let process_id = tg build --detach $path | str trim
tg wait $process_id

# First push - should push the process.
let output = tg --no-quiet push --commands --no-outputs $process_id | complete
success $output
snapshot ($output.stderr | lines | parse --regex '(?<m>transferred \d+ processes)' | get m | uniq | str join "\n") 'transferred 1 processes'

# Index both sides.
tg index
tg --url $remote.url index

# Second push without force - should not transfer the indexed process.
let output = tg --no-quiet push --commands --no-outputs $process_id | complete
success $output
snapshot ($output.stderr | lines | parse --regex '(?<m>transferred \d+ processes)' | get m | uniq | str join "\n") 'transferred 0 processes'

# Third push with force - should transfer the process again.
let output = tg --no-quiet push --force --commands --no-outputs $process_id | complete
success $output
snapshot ($output.stderr | lines | parse --regex '(?<m>transferred \d+ processes)' | get m | uniq | str join "\n") 'transferred 1 processes'
