use ../../test.nu *

# The tg.file template literal strips the leading indentation from a multiline file and the text matches the snapshot.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => tg.file`\n\tHello, World!\n`.then((f) => f.text);
	'
}

let output = tg build $path
snapshot $output
