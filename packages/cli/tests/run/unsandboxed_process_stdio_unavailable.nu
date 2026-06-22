use ../../test.nu *

# Accessing the stdin, stdout, or stderr of an unsandboxed process whose stdio is set to null fails with an unavailable error.

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
			.stdio("null");
		let input = tg.encoding.utf8.encode("hello");
		await process.stdin.write(input);
	}
'
snapshot ($stdin | redact | normalize_ids) '
	error an error occurred
	-> the process failed
	   id = <process>
	-> stdin is not available
	   ╭─[<path>/tangram.ts:6:22]
	 5 │     let input = tg.encoding.utf8.encode("hello");
	 6 │     await process.stdin.write(input);
	   ·                         ▲
	   ·                         ╰── stdin is not available
	 7 │ }
	   ╰────

'

let stdout = run '
	export default async function () {
		let process = await tg
			.spawn`true`
			.stdio("null");
		await process.stdout.read();
	}
'
snapshot ($stdout | redact | normalize_ids) '
	error an error occurred
	-> the process failed
	   id = <process>
	-> stdout is not available
	   ╭─[<path>/tangram.ts:5:23]
	 4 │         .stdio("null");
	 5 │     await process.stdout.read();
	   ·                          ▲
	   ·                          ╰── stdout is not available
	 6 │ }
	   ╰────

'

let stderr = run '
	export default async function () {
		let process = await tg
			.spawn`true`
			.stdio("null");
		await process.stderr.read();
	}
'
snapshot ($stderr | redact | normalize_ids) '
	error an error occurred
	-> the process failed
	   id = <process>
	-> stderr is not available
	   ╭─[<path>/tangram.ts:5:23]
	 4 │         .stdio("null");
	 5 │     await process.stderr.read();
	   ·                          ▲
	   ·                          ╰── stderr is not available
	 6 │ }
	   ╰────

'
