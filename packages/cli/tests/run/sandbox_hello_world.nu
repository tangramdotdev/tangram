use ../lib/test.nu *

# A sandboxed command runs and returns its output.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			console.log("Hello, World!");
		};
	'
}

let output = tg run --sandbox $path
assert ($output == "Hello, World!")
