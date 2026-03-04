use ../../test.nu *

let server = spawn

let foo = artifact {
	foo: {
		tangram.ts: '
			import bar from "../bar";
			export default () => bar();
			export const failure = () => tg.assert(false, "failure in foo");
		'
	}
	bar: {
		tangram.ts: '
			import { failure } from "../foo";
			export default () => failure();
		'
	}
}
tg tag foo ($foo | path join 'foo')

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
	-> Uncaught Error: failure in foo
	   ╭─[./tangram.ts:2:22]
	 1 │ import foo from "foo";
	 2 │ export default () => foo();
	   ·                      ▲
	   ·                      ╰── Uncaught Error: failure in foo
	   ╰────
	   ╭─[foo:tangram.ts:2:22]
	 1 │ import bar from "../bar";
	 2 │ export default () => bar();
	   ·                      ▲
	   ·                      ╰── Uncaught Error: failure in foo
	 3 │ export const failure = () => tg.assert(false, "failure in foo");
	   ╰────
	   ╭─[foo:../bar/tangram.ts:2:22]
	 1 │ import { failure } from "../foo";
	 2 │ export default () => failure();
	   ·                      ▲
	   ·                      ╰── Uncaught Error: failure in foo
	   ╰────
	   ╭─[foo:../foo/tangram.ts:3:33]
	 2 │ export default () => bar();
	 3 │ export const failure = () => tg.assert(false, "failure in foo");
	   ·                                 ▲
	   ·                                 ╰── Uncaught Error: failure in foo
	   ╰────
'

let output = do { cd $path; tg run } | complete
failure $output
let stderr = $output.stderr | lines | skip 1 | str join "\n"
let stderr = $stderr | str replace -ar 'pcs_00[0-9a-z]{26}' 'PROCESS'
assert equal $stderr $sandbox_stderr
