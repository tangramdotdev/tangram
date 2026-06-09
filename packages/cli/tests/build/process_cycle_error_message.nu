use ../../test.nu *

# A self-referential build fails with a process cycle error and the formatted error message matches the snapshot.

let server = spawn

let path = artifact {
	tangram.ts: '
		export let x = () => tg.build(x);
	'
}

let output = tg build ($path + '#x') | complete
failure $output

# Extract only the first cycle error block from the chain.
let lines = ($output.stderr | lines)
mut cycle_lines = []
mut recording = false
for line in $lines {
	if ($line | str starts-with '-> adding this child process creates a cycle') {
		if $recording {
			break
		}
		$recording = true
	}
	if $recording {
		$cycle_lines = ($cycle_lines | append $line)
	}
}
if ($cycle_lines | is-empty) {
	error make { msg: 'expected a cycle error message' }
}
let cycle_message = ($cycle_lines | str join "\n") | redact | normalize_ids

snapshot $cycle_message '
	-> adding this child process creates a cycle
	   <process> tried to add child <process>
'
