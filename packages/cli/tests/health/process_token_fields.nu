use ../../test.nu *

# A process authenticated client may request only the diagnostics and version health fields.

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

# The full health is unauthorized for a process token.
let output = tg --token $token health | complete
failure $output
assert ($output.stderr | str contains 'unauthorized') "the full health should be unauthorized"

# The diagnostics and version fields are allowed for a process token.
let health = tg --token $token health --fields diagnostics,version | from json
assert equal ($health | columns) [diagnostics version] "the allowed fields should be returned"

tg cancel $parent.process $parent.lease
tg wait $parent.process
