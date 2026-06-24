use ../../test.nu *

# A single-line tg template literal interpolates an imported file artifact and matches the snapshot.

let server = spawn

let path = artifact {
	tangram.ts: '
		import file from "./hello.txt";
		export default function () { return tg`cat ${file}`; }
	'
	hello.txt: 'Hello, World!'
}

let output = tg build $path
snapshot $output
