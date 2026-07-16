use ../../test.nu *

# A self-referential build fails with a process cycle error and the formatted error message matches the snapshot.

let server = spawn

let path = artifact {
	tangram.ts: '
		export function x() { return tg.build(x); }
	'
}

let output = tg build ($path + '#x') | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> failed to get a cached process
	-> adding this child process creates a cycle
	   child = pcs_0000000000000000000000000000
	   parent = pcs_0000000000000000000000000000

'
