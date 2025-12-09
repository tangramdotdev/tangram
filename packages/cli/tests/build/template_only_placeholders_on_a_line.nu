use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		import file from "./hello.txt";
		export default () => tg`
			${file}${file}
		`;
	'
	hello.txt: 'Hello, World!'
}

let output = tg build $path
snapshot $output
