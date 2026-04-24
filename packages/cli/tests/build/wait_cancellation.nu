use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			while (true) {
				await tg.sleep(1);
			}
		};
	'
}

let process = tg build -dv $path | from json
tg cancel $process.process $process.token
let output = tg output $process.process | complete
failure $output
let stderr = $output.stderr | ansi strip | str trim | str replace -r 'id = pcs_[a-z0-9]+' 'id = <process>'
snapshot $stderr '
	error an error occurred
	-> failed to get the process output
	   id = <process>
	-> the process was canceled
'

let id = job spawn {
	tg build $path | complete
};
job kill $id

let output = tg output $process.process | complete
failure $output
let stderr = $output.stderr | ansi strip | str trim | str replace -r 'id = pcs_[a-z0-9]+' 'id = <process>'
snapshot $stderr '
	error an error occurred
	-> failed to get the process output
	   id = <process>
	-> the process was canceled
'

let path = artifact {
	tangram.ts: '
		export default async () => {
			await Promise.race([
				tg.sleep(0),
				f(), 
			]);
		};

		let f = async () => {
			await tg.sleep(100);
			console.log("after sleep");
		}
	'
}
let id = tg build -d $path
tg wait $id
let log = tg log $id
assert equal $log ''
