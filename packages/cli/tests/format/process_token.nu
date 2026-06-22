use ../../test.nu *

# A process authenticated client may not format.

let server = spawn --busybox

# Run a sandboxed command that logs its process token and stays alive.
let path = artifact {
	tangram.ts: '
		import busybox from "busybox";

		export default async function () {
			await tg.run`echo "$TANGRAM_TOKEN" && sleep 60`.env(tg.build(busybox)).sandbox();
		}
	'
}
let parent = tg build --detach --verbose $path | from json
wait_until { (tg log $parent.process | str trim | str length) > 0 } "the process should log its token"
let token = tg log $parent.process | str trim

# Formatting with a process token is unauthorized.
let dir = mktemp --directory
'export default   "x"' | save ($dir | path join tangram.ts)
let output = tg --token $token format $dir | complete
failure $output
snapshot ($output.stderr | redact $dir) '
	error an error occurred
	-> failed to format
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to format
	-> unauthorized

'

tg cancel $parent.process $parent.lease
tg wait $parent.process
