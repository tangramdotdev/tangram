use ../../test.nu *

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
