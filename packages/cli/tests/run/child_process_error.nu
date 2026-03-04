use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.run(foo);
		}

		export function foo() {
			throw tg.error("error");
		}
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
	-> the child process failed
	   id = PROCESS
	   ╭─[./tangram.ts:2:9]
	 1 │ export default async function () {
	 2 │     return await tg.run(foo);
	   ·            ▲
	   ·            ╰── the child process failed
	 3 │ }
	   ╰────
	-> error
	   ╭─[./tangram.ts:6:11]
	 5 │ export function foo() {
	 6 │     throw tg.error("error");
	   ·              ▲
	   ·              ╰── error
	 7 │ }
	   ╰────
'

let output = do { cd $path; tg run } | complete
failure $output
let stderr = $output.stderr | lines | skip 1 | str join "\n"
let stderr = $stderr | str replace -ar 'pcs_00[0-9a-z]{26}' 'PROCESS'
assert equal $stderr $sandbox_stderr
