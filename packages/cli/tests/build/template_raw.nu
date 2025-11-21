use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => tg.Template.raw`\n\tHello, World!\n`;
	'
}

let output = run tg build $path
snapshot $output
