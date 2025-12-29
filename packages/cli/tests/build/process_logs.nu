use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			console.log("stdout line 1");
			console.error("stderr line 1");
			console.log("stdout line 2");
			console.error("stderr line 2");
		};
	'
}

let output = tg build -d $path | from json
let process_id = $output.process
tg wait $process_id

let combined = tg process log $process_id o+e>| complete
let stdout = tg process log --stream stdout $process_id | complete
let stderr = tg process log --stream stderr $process_id | complete

snapshot $combined.stdout '
	stdout line 1
	stdout line 2
	stderr line 1
	stderr line 2

'

snapshot $stdout.stdout '
	stdout line 1
	stdout line 2

'

snapshot $stderr.stderr '
	stderr line 1
	stderr line 2

'
