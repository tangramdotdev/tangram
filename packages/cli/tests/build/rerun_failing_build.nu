use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => { throw new Error("whoops"); };
	'
}

cd $path
let output = tg build | complete
assert equal $output.exit_code 1
let first_output = $output.stderr 
	| str replace --regex --all 'pcs_[a-z0-9]+' "PROCESS"
	| str replace --regex --all '\s06[a-z0-9]+' ""
snapshot $first_output '
	info PROCESS
	error an error occurred
	-> the process failed
	   id = PROCESS
	-> whoops
	   ╭─[./tangram.ts:1:30]
	 1 │ export default () => { throw new Error("whoops"); };
	   ·                              ▲
	   ·                              ╰── whoops
	   ╰────

'

let output = tg build | complete
assert equal $output.exit_code 1
let second_output = $output.stderr 
	| str replace --regex --all 'pcs_\w+' "PROCESS"
assert equal $first_output $second_output
