use ../../test.nu *

# Accessing the stdin, stdout, or stderr of a sandboxed process whose stdio is set to null fails with an unavailable error.

let server = spawn

def run_source [source: string] {
	let path = artifact { tangram.ts: $source }
	let output = tg run $path | complete
	failure $output
	{
		path: $path,
		stderr: $output.stderr,
	}
}

let stdin = run_source '
	export default async function () {
		let process = await tg
			.spawn`true`
			.stdio("null")
			.sandbox();
		let input = tg.encoding.utf8.encode("hello");
		await process.stdin.write(input);
	}
'
snapshot --normalize-ids --redact $stdin.path $stdin.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> stdin is not available
	   ╭─[<redacted>/tangram.ts:7:22]
	 6 │     let input = tg.encoding.utf8.encode("hello");
	 7 │     await process.stdin.write(input);
	   ·                         ▲
	   ·                         ╰── stdin is not available
	 8 │ }
	   ╰────

'

let stdout = run_source '
	export default async function () {
		let process = await tg
			.spawn`true`
			.stdio("null")
			.sandbox();
		await process.stdout.read();
	}
'
snapshot --normalize-ids --redact $stdout.path $stdout.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> stdout is not available
	   ╭─[<redacted>/tangram.ts:6:23]
	 5 │         .sandbox();
	 6 │     await process.stdout.read();
	   ·                          ▲
	   ·                          ╰── stdout is not available
	 7 │ }
	   ╰────

'

let stderr = run_source '
	export default async function () {
		let process = await tg
			.spawn`true`
			.stdio("null")
			.sandbox();
		await process.stderr.read();
	}
'
snapshot --normalize-ids --redact $stderr.path $stderr.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> stderr is not available
	   ╭─[<redacted>/tangram.ts:6:23]
	 5 │         .sandbox();
	 6 │     await process.stderr.read();
	   ·                          ▲
	   ·                          ╰── stderr is not available
	 7 │ }
	   ╰────

'
