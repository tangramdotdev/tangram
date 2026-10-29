use ../../util/test.nu *

# The tree command renders the processes that ran in a sandbox.

let local = server spawn --config { advanced: { checkpoints: true } }

let path = artifact {
	tangram.ts: 'export default function () { return 42; }'
}

# Render the tree, which follows the sandbox's processes until the sandbox is destroyed, and wait for it to start following.
let sandbox = tg sandbox create --no-tokens | referent node
let subscribed = checkpoint watch sandbox.processes.subscribed --params { sandbox: $sandbox }
let tree = job spawn {
	let job_id = job id
	let output = tg tree $sandbox --depth 2 | complete
	$output | job send --tag $job_id 0
}
checkpoint pass $subscribed
checkpoint unwatch $subscribed

# Run a process in the sandbox while the tree follows it, then destroy the sandbox.
let output = tg run $"--sandbox=($sandbox)" $path
snapshot $output "42"
tg sandbox destroy $sandbox

let output = job recv --tag $tree --timeout 10sec
success $output
snapshot --normalize-ids ($output.stdout | str trim) '
	sbx_0000000000000000000000000000
	└╴✓ fil_010000000000000000000000000000000000000000000000000000#default
	  ├╴output: 42
	  └╴command: map
'
