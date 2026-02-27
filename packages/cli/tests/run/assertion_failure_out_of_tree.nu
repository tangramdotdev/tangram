use ../../test.nu *

let server = spawn

let path = artifact {
	foo: {
		tangram.ts: '
			import bar from "../bar";
			export default () => tg.run(bar);
		'
	}
	bar: {
		tangram.ts: '
			export default () => tg.assert(false);
		'
	}
}

let sandbox_output = do { cd $path; tg run ./foo --sandbox } | complete
failure $sandbox_output
let sandbox_stderr = $sandbox_output.stderr | lines | skip 1 | str join "\n"
let sandbox_stderr = $sandbox_stderr | str replace -ar 'pcs_00[0-9a-z]{26}' 'PROCESS'
snapshot $sandbox_stderr '
	error an error occurred
	-> the process failed
	   id = PROCESS
	-> the child process failed
	   id = PROCESS
	-> Uncaught Error: failed assertion
	   ╭─[./bar/tangram.ts:1:25]
	 1 │ export default () => tg.assert(false);
	   ·                         ▲
	   ·                         ╰── Uncaught Error: failed assertion
	   ╰────
'
let output = do { cd $path; tg run ./foo } | complete
failure $output
let stderr = $output.stderr | lines | skip 1 | str join "\n"
let stderr = $stderr | str replace -ar 'pcs_00[0-9a-z]{26}' 'PROCESS'
assert equal $stderr $sandbox_stderr
