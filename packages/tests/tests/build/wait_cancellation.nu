use ../../util/test.nu *

# Getting the output of a process that is canceled with `tg cancel` while it runs fails with a canceled error.

let local = server spawn

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
tg process status --until started $process.process | ignore
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
