use ../../test.nu *

let server = spawn

let path = artifact {
	foo: {
		tangram.ts: '
			import foo from "../bar";
			export default () => foo();
		'
	}
	bar: {
		tangram.ts: '
			export default () => tg.assert(false, "error")
		'
	}
}

let sandbox_output = do { cd $path; tg run ./foo --sandbox } | complete
print $sandbox_output
failure $sandbox_output
let sandbox_stderr = $sandbox_output.stderr | lines | skip 1 | str join "\n"
let sandbox_stderr = $sandbox_stderr | str replace -ar 'pcs_00[0-9a-z]{26}' 'PROCESS'
snapshot $sandbox_stderr '
	error an error occurred
	-> the process failed
	   id = PROCESS
	-> Uncaught Error: error
	   ╭─[./foo/tangram.ts:2:22]
	 1 │ import foo from "../bar";
	 2 │ export default () => foo();
	   ·                      ▲
	   ·                      ╰── Uncaught Error: error
	   ╰────
	   ╭─[./bar/tangram.ts:1:25]
	 1 │ export default () => tg.assert(false, "error")
	   ·                         ▲
	   ·                         ╰── Uncaught Error: error
	   ╰────
'

let output = do { cd $path; tg run ./foo } | complete
failure $output
let stderr = $output.stderr | lines | skip 1 | str join "\n"
let stderr = $stderr | str replace -ar 'pcs_00[0-9a-z]{26}' 'PROCESS'
assert equal $stderr $sandbox_stderr
