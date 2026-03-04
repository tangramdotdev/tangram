use ../../test.nu *

# When a process exits with a non-zero exit code (1 < code < 128), `tg run` should fail with an error mentioning the exit code.

let server = spawn --busybox

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default () => tg.build`exit 42`.env(tg.build(busybox));
	'
}

let sandbox_output = tg run $path --sandbox | complete
failure $sandbox_output
assert ($sandbox_output.stderr | str contains "exited with code 42")

let output = tg run $path | complete
failure $output
assert ($output.stderr | str contains "exited with code 42")
