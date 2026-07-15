use ../../test.nu *

# A process authenticated client may not list watches.

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

# Listing watches with a process token is unauthorized.
let output = tg --token $token watch list | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to list the watches
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to list the watches
	-> unauthorized

'

tg cancel $parent.process $parent.lease
tg wait $parent.process
