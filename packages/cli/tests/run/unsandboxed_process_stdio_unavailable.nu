use ../../test.nu *

# Accessing the stdin, stdout, or stderr of an unsandboxed process whose stdio is set to null fails with an unavailable error.

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
			.stdio("null");
		let input = tg.encoding.utf8.encode("hello");
		await process.stdin.write(input);
	}
'
snapshot --normalize-ids --redact $stdin.path $stdin.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> stdin is not available
	   ╭─[<redacted>/tangram.ts:6:22]
	 5 │     let input = tg.encoding.utf8.encode("hello");
	 6 │     await process.stdin.write(input);
	   ·                         ▲
	   ·                         ╰── stdin is not available
	 7 │ }
	   ╰────

'

let stdout = run_source '
	export default async function () {
		let process = await tg
			.spawn`true`
			.stdio("null");
		await process.stdout.read();
	}
'
snapshot --normalize-ids --redact $stdout.path $stdout.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> stdout is not available
	   ╭─[<redacted>/tangram.ts:5:23]
	 4 │         .stdio("null");
	 5 │     await process.stdout.read();
	   ·                          ▲
	   ·                          ╰── stdout is not available
	 6 │ }
	   ╰────

'

let stderr = run_source '
	export default async function () {
		let process = await tg
			.spawn`true`
			.stdio("null");
		await process.stderr.read();
	}
'
snapshot --normalize-ids --redact $stderr.path $stderr.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> stderr is not available
	   ╭─[<redacted>/tangram.ts:5:23]
	 4 │         .stdio("null");
	 5 │     await process.stderr.read();
	   ·                          ▲
	   ·                          ╰── stderr is not available
	 6 │ }
	   ╰────

'
