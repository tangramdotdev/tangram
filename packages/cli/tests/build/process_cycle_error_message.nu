use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export let x = () => tg.build(x);
	'
}

let output = tg build ($path + '#x') | complete
failure $output

# Extract only the cycle error message (the last error in the chain).
let cycle_message = $output.stderr
	| lines
	| skip while { |line| not ($line | str starts-with '-> adding this child process creates a cycle') }
	| str join "\n"
	| str replace --all --regex 'pcs_[a-z0-9]+' '<process_id>'

snapshot $cycle_message '
	-> adding this child process creates a cycle
	   <process_id> tried to add child <process_id>
'
