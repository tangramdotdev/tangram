use ../../test.nu *

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
	| str replace --regex --all 'pcs_[a-z0-9]+' "PROCESS"
snapshot $first_output '
	error an error occurred
	-> the process failed
	   id = PROCESS
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
	| str replace --regex --all 'pcs_\w+' "PROCESS"
assert equal $first_output $second_output
