use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			console.log("Hello, World!");
		};
	'
}

let output = run tg run $path
snapshot $output
