use ../../test.nu *

# A process authenticated client may not get a tag.

let server = spawn --busybox

# Create a tag so the failure cannot be attributed to a missing tag.
let path = artifact 'test'
let id = tg checkin $path
tg tag put test $id

# Run a sandboxed command that logs its process token and stays alive.
let module = artifact {
	tangram.ts: '
		import busybox from "busybox";

		export default async function () {
			await tg.run`echo "$TANGRAM_TOKEN" && sleep 60`.env(tg.build(busybox)).sandbox();
		}
	'
}
let parent = tg build --detach --verbose $module | from json
wait_until { (tg log $parent.process | str trim | str length) > 0 } "the process should log its token"
let token = tg log $parent.process | str trim

# Getting a tag with a process token is unauthorized.
let output = tg --token $token tag get test | complete
failure $output
assert ($output.stderr | str contains "unauthorized") "the tag get should be unauthorized"

tg cancel $parent.process $parent.lease
tg wait $parent.process
