use ../../test.nu *

# A tg template literal whose line contains only adjacent artifact placeholders renders them concatenated and matches the snapshot.

let server = spawn

let path = artifact {
	tangram.ts: '
		import file from "./hello.txt";
		export default function () { return tg`
			${file}${file}
		`; }
	'
	hello.txt: 'Hello, World!'
}

let output = tg build $path
snapshot $output
