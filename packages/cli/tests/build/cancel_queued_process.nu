use ../../test.nu *

# Cancelling a process that is queued behind the runner concurrency limit cancels it before it starts.

let server = spawn --config { runner: { concurrency: 1 } }

# Occupy the single runner slot.
let blocker_path = artifact {
	tangram.ts: '
		export default async function () {
			while (true) {
				await tg.sleep(1);
			}
		}
	'
}
let blocker = tg build --detach --verbose $blocker_path | from json
wait_until { (tg status --timeout 0 $blocker.process | from json) == ["started"] } "the blocker should start"

# Spawn a second process that queues behind the blocker.
let queued_path = artifact {
	tangram.ts: '
		export default async function () {
			while (true) {
				await tg.sleep(0.9);
			}
		}
	'
}
let queued = tg build --detach --verbose $queued_path | from json
assert ((tg status --timeout 0 $queued.process | from json) != ["started"]) "the process should be queued, not started"

# Cancel the queued process.
tg cancel $queued.process $queued.lease
let output = tg output $queued.process | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to get the process output
	   id = <process>
	-> the process was canceled

'

# The blocker is unaffected.
assert equal (tg status --timeout 0 $blocker.process | from json) ["started"] "the blocker should still be running"
tg cancel $blocker.process $blocker.lease
tg wait $blocker.process
