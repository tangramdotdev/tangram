use ../../test.nu *

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			console.log("Hello, World!");
		};
	'
}

let output = tg run --sandbox $path
assert ($output == "Hello, World!")
