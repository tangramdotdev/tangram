use ../../../test.nu *

# A tg.download that responds with an error status fails with the reason on the CLI.

skip_if_offline

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.download("http://www.example.com/does-not-exist");
		}
	'
}

let output = tg build $path | complete
failure $output
snapshot --normalize --redact $path $output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> the child process failed
	   id = pcs_0011111111111111111111111111
	   ╭─[<redacted>/tangram.ts:2:9]
	 1 │ export default async function () {
	 2 │     return await tg.download("http://www.example.com/does-not-exist");
	   ·            ▲
	   ·            ╰── the child process failed
	 3 │ }
	   ╰────
	-> expected a success status
	   url = http://www.example.com/does-not-exist
	-> HTTP status client error (404 Not Found) for url (http://www.example.com/does-not-exist)

'
