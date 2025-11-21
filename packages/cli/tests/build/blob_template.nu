use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => tg.blob`\n\tHello, World!\n`.then((b) => b.text());
	'
}

let output = run tg build $path
snapshot $output
