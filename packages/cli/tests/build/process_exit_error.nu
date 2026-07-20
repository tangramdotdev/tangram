use ../../test.nu *

# A build whose command exits with a non-zero status fails and the error is loaded successfully rather than reporting a failure to load the error.

let server = spawn --busybox

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default function () { return tg.build`exit 1`.env(tg.build(busybox)); }
	'
}

let output = tg build $path | complete
failure $output
snapshot --normalize-ids --redact $path $output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> the child process failed
	   id = pcs_0011111111111111111111111111
	-> the process exited with code 1

'
