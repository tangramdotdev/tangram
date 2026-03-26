use ../../test.nu *

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
snapshot ($output.stdout | str trim -r -c "\n") 'Hello, World!'
