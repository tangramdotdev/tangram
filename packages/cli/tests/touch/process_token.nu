use ../../test.nu *

# A process authenticated client may touch an object but may not touch a process.

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

# Touching a process with a process token is unauthorized.
let output = tg --token $token process touch $parent.process | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to touch the process
	   id = <process>
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to touch the process
	   id = <process>
	-> unauthorized

'

# Touching an object with a process token is allowed.
let id = tg --token $token put 'tg.file("object for token")' | str trim
let output = tg --token $token object touch $id | complete
success $output

tg cancel $parent.process $parent.lease
tg wait $parent.process
