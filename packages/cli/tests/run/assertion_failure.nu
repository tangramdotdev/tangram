use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		import foo from "./foo.tg.ts";
		export default () => foo();
	',
	foo.tg.ts: '
		export default () => tg.assert(false);
	',
}

let sandbox_output = do { cd $path; tg run --sandbox } | complete
failure $sandbox_output
let sandbox_stderr = $sandbox_output.stderr | lines | skip 1 | str join "\n"
let sandbox_stderr = $sandbox_stderr | str replace -ar 'pcs_00[0-9a-z]{26}' 'PROCESS'
snapshot $sandbox_stderr '
	error an error occurred
	-> the process failed
	   id = PROCESS
	-> Uncaught Error: failed assertion
	   ╭─[./tangram.ts:2:22]
	 1 │ import foo from "./foo.tg.ts";
	 2 │ export default () => foo();
	   ·                      ▲
	   ·                      ╰── Uncaught Error: failed assertion
	   ╰────
	   ╭─[./foo.tg.ts:1:25]
	 1 │ export default () => tg.assert(false);
	   ·                         ▲
	   ·                         ╰── Uncaught Error: failed assertion
	   ╰────
'

let output = do { cd $path; tg run } | complete
failure $output
let stderr = $output.stderr | lines | skip 1 | str join "\n"
let stderr = $stderr | str replace -ar 'pcs_00[0-9a-z]{26}' 'PROCESS'
assert equal $stderr $sandbox_stderr
