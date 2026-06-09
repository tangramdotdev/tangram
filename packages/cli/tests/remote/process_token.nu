use ../../test.nu *

# A process authenticated client may not put a remote.

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

# Putting a remote with a process token is unauthorized.
let output = tg --token $token remote put upstream "http://localhost:9999" | complete
failure $output
assert ($output.stderr | str contains 'unauthorized') "the put should be unauthorized"

tg cancel $parent.process $parent.lease
tg wait $parent.process
