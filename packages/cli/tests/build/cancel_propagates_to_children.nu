use ../lib/test.nu *

# Cancelling a process also cancels its running children.

let local = server spawn --config { advanced: { checkpoints: true } }

let path = artifact {
	tangram.ts: '
		export async function child() {
			while (true) {
				await tg.sleep(1);
			}
		}

		export default async function () {
			await tg.build(child);
		}
	'
}

# Start the parent and wait for the child to start after it.
let watch = tg checkpoint watch runner.process.start | from json | get watch
let parent = tg build --detach --verbose $path | from json
tg checkpoint wait runner.process.start $watch 0 | ignore
tg checkpoint continue runner.process.start $watch 0
let child = tg checkpoint wait runner.process.start $watch 1 | from json | get params.process
tg checkpoint continue runner.process.start $watch 1
tg checkpoint unwatch runner.process.start $watch

# Cancel the parent.
tg cancel $parent.process $parent.lease

# The child is canceled.
let output = tg output $child | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to get the process output
	   id = pcs_0000000000000000000000000000
	-> failed to run the process
	   process = pcs_0000000000000000000000000000
	-> the process was canceled

'
