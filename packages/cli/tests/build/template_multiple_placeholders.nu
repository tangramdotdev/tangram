use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		import file1 from "./hello.txt";
		import file2 from "./hello.txt";
		import file3 from "./hello.txt";
		export default () => tg`
			cat\t${file1}\t${file1}
		`;
	'
	hello.txt: 'Hello, World!'
}

let output = run tg build $path
snapshot $output
