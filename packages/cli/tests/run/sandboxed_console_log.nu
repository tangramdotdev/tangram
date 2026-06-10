use ../../test.nu *

# Console.log output from a sandboxed run is captured on stdout.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			console.log("Hello, World!");
		}
	',
}

let output = tg run --sandbox $path | complete
success $output
snapshot $output.stdout '
	Hello, World!

'
