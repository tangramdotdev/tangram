use ../../test.nu *

# Accessing the stdin, stdout, or stderr of a sandboxed process whose stdio is set to null fails with an unavailable error.

let server = spawn

def run [source: string] {
	let path = artifact { tangram.ts: $source }
	let output = tg run $path | complete
	failure $output
	$output.stderr | redact $path
}

let stdin = run '
	export default async function () {
		let process = await tg
			.spawn`true`
			.stdio("null")
			.sandbox();
		let input = tg.encoding.utf8.encode("hello");
		await process.stdin.write(input);
	}
'
snapshot ($stdin | redact | normalize_ids) '
	error an error occurred
	-> the process failed
	   id = <process>
	-> stdin is not available
	   ╭─[<path>/tangram.ts:7:22]
	 6 │     let input = tg.encoding.utf8.encode("hello");
	 7 │     await process.stdin.write(input);
	   ·                         ▲
	   ·                         ╰── stdin is not available
	 8 │ }
	   ╰────

'

let stdout = run '
	export default async function () {
		let process = await tg
			.spawn`true`
			.stdio("null")
			.sandbox();
		await process.stdout.read();
	}
'
snapshot ($stdout | redact | normalize_ids) '
	error an error occurred
	-> the process failed
	   id = <process>
	-> stdout is not available
	   ╭─[<path>/tangram.ts:6:23]
	 5 │         .sandbox();
	 6 │     await process.stdout.read();
	   ·                          ▲
	   ·                          ╰── stdout is not available
	 7 │ }
	   ╰────

'

let stderr = run '
	export default async function () {
		let process = await tg
			.spawn`true`
			.stdio("null")
			.sandbox();
		await process.stderr.read();
	}
'
snapshot ($stderr | redact | normalize_ids) '
	error an error occurred
	-> the process failed
	   id = <process>
	-> stderr is not available
	   ╭─[<path>/tangram.ts:6:23]
	 5 │         .sandbox();
	 6 │     await process.stderr.read();
	   ·                          ▲
	   ·                          ╰── stderr is not available
	 7 │ }
	   ╰────

'
