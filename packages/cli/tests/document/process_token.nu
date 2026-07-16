use ../../test.nu *

# A process authenticated client may not document.

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

# Documenting with a process token is unauthorized.
let module = artifact {
	tangram.ts: '
		export default function () { return "x"; }
	'
}
let output = tg --token $token document $module | complete
failure $output
snapshot --normalize --redact $module $output.stderr '
	error an error occurred
	-> failed to get the reference
	   reference = <redacted>
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to get the reference
	   reference = <redacted>
	-> unauthorized

'

tg cancel $parent.process $parent.lease
tg wait $parent.process
