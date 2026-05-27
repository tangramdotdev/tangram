use ../../test.nu *

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
let lease = $output.lease

let output = tg signal -s KILL $process | complete
failure $output
assert ($output.stderr | str contains "required") "The error should mention the missing lease."

let output = tg signal --lease $lease -s KILL $process | complete
success $output

# Wait for the process to finish.
let output = tg wait $process | from json
snapshot -n wait $output '
	exit: 137

'
