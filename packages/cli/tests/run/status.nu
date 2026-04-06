use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => "hello";
	'
}

# Spawn a process and wait for it to finish.
let process = tg build -dv $path | from json
tg wait $process.process

# Get the status.
let output = tg process status $process.process | complete
success $output
snapshot -n status ($output.stdout | str trim) '["finished"]'
