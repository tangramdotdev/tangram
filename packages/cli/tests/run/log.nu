use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			console.log("hello from stdout");
		};
	'
}

# Build the process and wait for it to finish.
let process = tg build -dv $path | from json
let output = tg log $process.process | complete
success $output
snapshot -n log ($output.stdout | str trim) 'hello from stdout'
tg wait $process.process
