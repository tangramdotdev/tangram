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
let process = tg spawn --sandbox --mount $"($mount):/target" $path | str trim
let output = tg signal -s KILL $process | complete
success $output

# Wait for the process to finish.
let output = tg wait $process | from json
snapshot -n wait $output '
	exit: 137

'
