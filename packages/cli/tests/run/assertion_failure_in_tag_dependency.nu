use ../../test.nu *

let server = spawn

# Create and tag the foo dependency.
let foo_path = artifact {
	tangram.ts: '
		export default () => tg.assert(false, "error in foo");
	'
}
tg tag foo $foo_path

let path = artifact {
	tangram.ts: '
		import foo from "foo";
		export default () => foo();
	'
}

let sandbox_output = do { cd $path; tg run --sandbox } | complete
failure $sandbox_output
let sandbox_stderr = $sandbox_output.stderr | lines | skip 1 | str join "\n"
let sandbox_stderr = $sandbox_stderr | str replace -ar 'pcs_00[0-9a-z]{26}' 'PROCESS'
snapshot $sandbox_stderr '
	error an error occurred
	-> the process failed
	   id = PROCESS
	-> Uncaught Error: error in foo
	   ╭─[./tangram.ts:2:22]
	 1 │ import foo from "foo";
	 2 │ export default () => foo();
	   ·                      ▲
	   ·                      ╰── Uncaught Error: error in foo
	   ╰────
	   ╭─[foo:tangram.ts:1:25]
	 1 │ export default () => tg.assert(false, "error in foo");
	   ·                         ▲
	   ·                         ╰── Uncaught Error: error in foo
	   ╰────
'

let output = do { cd $path; tg run } | complete
failure $output
let stderr = $output.stderr | lines | skip 1 | str join "\n"
let stderr = $stderr | str replace -ar 'pcs_00[0-9a-z]{26}' 'PROCESS'
assert equal $stderr $sandbox_stderr
