use ../../test.nu *

# A module can import a text file and read its contents.

let server = spawn

let path = artifact {
	tangram.ts: '
		import file from "./hello.txt";
		export default function () { return file.text; }
	'
	hello.txt: 'Hello, World!'
}

let output = tg build $path
snapshot $output
