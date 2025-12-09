use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			let a = "string!";
			return tg.blob`\n\tHello, World!\n\t${a}\n`.then((f) => f.text());
		}
	'
}

let output = tg build $path
snapshot $output
