use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => tg.file`\n\tHello, World!\n`.then((f) => f.text);
	'
}

let output = tg build $path
snapshot $output
