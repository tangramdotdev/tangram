use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			await tg.sleep(60);
		};
	'
}

# Spawn a long-running process.
let process = tg run --sandbox $path | str trim

# Signal the process.
let output = tg signal $process | complete
success $output

# Wait for the process to finish.
let output = tg wait $process
snapshot -n wait $output
