use ../../test.nu *

# Getting the output of a canceled process fails with a canceled
# error, whether the process is canceled explicitly with `tg cancel` or by killing
# the client that started the build.

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

let process = tg build --detach --verbose $path | from json
tg cancel $process.process $process.lease
let output = tg output $process.process | complete
failure $output
let stderr = $output.stderr | ansi strip | str trim | redact | normalize_ids
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
let stderr = $output.stderr | ansi strip | str trim | redact | normalize_ids
snapshot $stderr '
	error an error occurred
	-> failed to get the process output
	   id = <process>
	-> the process was canceled
'
