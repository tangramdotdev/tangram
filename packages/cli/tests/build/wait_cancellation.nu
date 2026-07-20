use ../../test.nu *

# Getting the output of a canceled process fails with a canceled
# error, whether the process is canceled explicitly with `tg cancel` or by killing
# the client that started the build.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			while (true) {
				await tg.sleep(1);
			}
		}
	'
}

let process = tg build --detach --verbose $path | from json
tg cancel $process.process $process.lease
let output = tg output $process.process | complete
failure $output
let stderr = $output.stderr | ansi strip | str trim
snapshot --normalize-ids $stderr '
	error an error occurred
	-> failed to get the process output
	   id = pcs_0000000000000000000000000000
	-> failed to run the process
	   process = pcs_0000000000000000000000000000
	-> the process was canceled
'

let id = job spawn {
	tg build $path | complete
};
job kill $id

let output = tg output $process.process | complete
failure $output
let stderr = $output.stderr | ansi strip | str trim
snapshot --normalize-ids $stderr '
	error an error occurred
	-> failed to get the process output
	   id = pcs_0000000000000000000000000000
	-> failed to run the process
	   process = pcs_0000000000000000000000000000
	-> the process was canceled
'
