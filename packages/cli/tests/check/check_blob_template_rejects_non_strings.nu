use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		import file from "./file.txt";
		export default () => tg.blob`\n\t${file}\n`;
	'
	file.txt: 'Hello, world!'
}

# Check.
let output = do { cd $path; tg check . } | complete
failure $output
