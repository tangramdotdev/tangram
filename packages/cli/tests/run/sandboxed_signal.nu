use ../../test.nu *

# The tg signal command delivers a KILL signal to a running sandboxed process and the process finishes with the corresponding exit status.

let server = spawn
let mount = artifact {}
let path = artifact {
	tangram.ts: '
		export default async () => {
			await tg.sleep(10);
			return 42;
		};
	'
}
let output = tg spawn --sandbox --mount $"($mount):/target" --verbose $path | from json
let process = $output.process

let output = tg signal --signal KILL $process | complete
success $output

# Wait for the process to finish.
let output = tg wait $process | from json
snapshot --name wait $output '
	exit: 137

'
