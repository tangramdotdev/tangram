use ../../test.nu *

# Cancelling a process also cancels its running children.

let server = spawn

let path = artifact {
	tangram.ts: '
		export const child = async () => {
			while (true) {
				await tg.sleep(1);
			}
		};

		export default async () => {
			await tg.build(child);
		};
	'
}

let parent = tg build --detach --verbose $path | from json

# Wait for the child process to spawn.
wait_until { (tg process children $parent.process | from json | length) > 0 } "the child should spawn"
let child = tg process children $parent.process | from json | get 0.process

# Cancel the parent.
tg cancel $parent.process $parent.lease

# The child is canceled.
let output = tg output $child | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to get the process output
	   id = <process>
	-> the process was canceled

'
