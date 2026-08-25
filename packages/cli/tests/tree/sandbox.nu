use ../../test.nu *

# The tree command renders the processes that ran in a sandbox.

let server = server spawn

let path = artifact {
	tangram.ts: 'export default function () { return 42; }'
}

let sandbox = tg sandbox create | str trim
let tree = job spawn {
	let job_id = job id
	let output = tg tree $sandbox --depth 2 | complete
	$output | job send --tag $job_id 0
}
sleep 1sec
let output = tg run $"--sandbox=($sandbox)" $path
snapshot $output "42"
tg sandbox destroy $sandbox

let output = job recv --tag $tree --timeout 10sec
success $output
snapshot --normalize-ids ($output.stdout | str trim) '
	sbx_0000000000000000000000000000
	└╴✓ fil_010000000000000000000000000000000000000000000000000000#default
	  ├╴output: 42
	  └╴command: cmd_010000000000000000000000000000000000000000000000000000
'
