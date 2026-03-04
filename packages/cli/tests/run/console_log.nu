use ../../test.nu *

# The process stdout from console.log should appear on the terminal. The return value should also be printed.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			console.log("log line 1");
			console.log("log line 2");
			return "return value";
		};
	'
}

let sandbox_output = tg run $path --sandbox | complete
success $sandbox_output
assert ($sandbox_output.stdout | str contains "log line 1")
assert ($sandbox_output.stdout | str contains "log line 2")
assert ($sandbox_output.stdout | str contains "return value")

let output = tg run $path | complete
success $output
assert ($output.stdout | str contains "log line 1")
assert ($output.stdout | str contains "log line 2")
assert ($output.stdout | str contains "return value")
