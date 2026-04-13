use ../../test.nu *

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
