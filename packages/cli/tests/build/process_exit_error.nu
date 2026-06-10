use ../../test.nu *

# A build whose command exits with a non-zero status fails and the error is loaded successfully rather than reporting a failure to load the error.

let server = spawn --busybox

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default () => tg.build`exit 1`.env(tg.build(busybox));
	'
}

let output = tg build $path | complete
failure $output
assert not ($output.stderr | str contains "failed to load the error")
