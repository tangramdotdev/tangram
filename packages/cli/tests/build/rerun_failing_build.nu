use ../../test.nu *

# Rerunning a build whose command throws produces the same failure and identical error output on each run.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => { throw tg.error("whoops"); };
	'
}

cd $path
let output = tg build | complete
assert equal $output.exit_code 1
let first_output = $output.stderr 
	| redact | normalize_ids
snapshot $first_output '
	error an error occurred
	-> the process failed
	   id = <process>
	-> whoops
	   ╭─[./tangram.ts:1:33]
	 1 │ export default () => { throw tg.error("whoops"); };
	   ·                                 ▲
	   ·                                 ╰── whoops
	   ╰────

'

let output = tg build | complete
assert equal $output.exit_code 1
let second_output = $output.stderr 
	| redact | normalize_ids
assert equal $first_output $second_output
