use ../../lib/test.nu *
use ../../lib/http.nu *

# A tg.download that responds with an error status fails with the reason on the CLI.

let local = server spawn
let http = spawn_http_server { '/does-not-exist': { status: 404 } }

let path = artifact {
	tangram.ts: '
		export default async function (url: string) {
			return await tg.download(url);
		}
	'
}

let output = tg build $path $'($http.url)/does-not-exist' | complete
failure $output
snapshot --normalize --redact [$path $http.url] $output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> the child process failed
	   id = pcs_0011111111111111111111111111
	   name = download
	   ╭─[<redacted>/tangram.ts:2:9]
	 1 │ export default async function (url: string) {
	 2 │     return await tg.download(url);
	   ·            ▲
	   ·            ╰── the child process failed
	 3 │ }
	   ╰────
	-> expected a success status
	   url = <redacted>/does-not-exist
	-> HTTP status client error (404 Not Found) for url (<redacted>/does-not-exist)

'
