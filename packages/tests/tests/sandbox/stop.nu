use ../../util/test.nu *

# A stopped sandbox reports that it is stopped, remains readable, and rejects new processes.

# Shorten the control request timeout, because a spawn into a stopped sandbox fails once its control request times out.
let local = server spawn --config { control: { request_timeout: 1 } }

let sandbox = tg sandbox create --no-tokens | referent node
tg sandbox stop $sandbox

let data = tg sandbox get $sandbox | from json | get data
assert equal $data.status stopped "the sandbox should be stopped"

let ids = tg sandbox list --status stopped | from json | get id
assert ($sandbox in $ids) "listing stopped sandboxes should include the sandbox"
let ids = tg sandbox list --status started | from json | get id
assert ($sandbox not-in $ids) "listing started sandboxes should not include the sandbox"

let path = artifact {
	tangram.ts: '
		export default async function () {
			return "hello";
		}
	',
}
let output = tg run $"--sandbox=($sandbox)" $path | complete
failure $output "spawning into a stopped sandbox should fail"
assert ($output.stderr | str contains "the sandbox is not started")

# Stopping a stopped sandbox succeeds.
tg sandbox stop $sandbox
