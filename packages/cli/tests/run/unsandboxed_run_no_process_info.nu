use ../../test.nu *

# Running with --no-sandbox writes only the process's stdout and emits no process-info noise on stderr.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			console.log("hello");
		};
	',
}

let output = tg run --no-sandbox $path | complete
success $output
assert (($output.stdout | str trim) == "hello")
assert ($output.stderr == "")
