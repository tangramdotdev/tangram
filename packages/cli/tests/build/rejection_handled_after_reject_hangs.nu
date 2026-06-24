use ../../test.nu *

# A build fails with the rejection error rather than hanging when a promise is rejected and a catch handler is attached after the program would otherwise await forever.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let promise = new Promise(() => reject(new Error("kaboom")));
			promise.catch(() => {});
			await new Promise(() => {});
		}
	'
}

cd $path
let output = timeout 10s tg build | complete
failure $output
snapshot ($output.stderr | redact $path | normalize_ids) '
	error an error occurred
	-> the process failed
	   id = <process>
	-> reject is not defined
	   ╭─[./tangram.ts:3:10]
	 2 │     let promise = new Promise(() => reject(new Error("kaboom")));
	 3 │     promise.catch(() => {});
	   ·             ▲
	   ·             ╰── reject is not defined
	 4 │     await new Promise(() => {});
	   ╰────
	   ╭─[./tangram.ts:2:16]
	 1 │ export default async function () {
	 2 │     let promise = new Promise(() => reject(new Error("kaboom")));
	   ·                   ▲
	   ·                   ╰── reject is not defined
	 3 │     promise.catch(() => {});
	   ╰────
	   ╭─[./tangram.ts:2:34]
	 1 │ export default async function () {
	 2 │     let promise = new Promise(() => reject(new Error("kaboom")));
	   ·                                     ▲
	   ·                                     ╰── reject is not defined
	 3 │     promise.catch(() => {});
	   ╰────

'
