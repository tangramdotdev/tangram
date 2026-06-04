use ../../test.nu *

# The check command fails when a tg.blob template interpolates a value that is not a string, such as an imported file.

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
