use ../../test.nu *

# Rerunning a build whose command throws produces the same failure and identical error output on each run.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () { throw tg.error.sync("whoops"); }
	'
}

cd $path
let output = tg build | complete
assert equal $output.exit_code 1
let first_output = $output.stderr 
	| normalize | normalize --normalize-ids
snapshot $first_output '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> whoops
	   ╭─[./tangram.ts:1:45]
	 1 │ export default function () { throw tg.error.sync("whoops"); }
	   ·                                             ▲
	   ·                                             ╰── whoops
	   ╰────

'

let output = tg build | complete
assert equal $output.exit_code 1
let second_output = $output.stderr 
	| normalize | normalize --normalize-ids
assert equal $first_output $second_output
