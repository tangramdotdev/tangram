use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		import file from "./hello.txt";
		export default () => file.text();
	'
	hello.txt: 'Hello, World!'
}

let output = tg build $path | complete
success $output
snapshot $output.stdout
