use ../../test.nu *

# A tg.blob template literal interpolates a string placeholder into a multiline blob and the resulting text matches the snapshot.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			let a = "string!";
			return tg.blob`\n\tHello, World!\n\t${a}\n`.then((f) => f.text);
		}
	'
}

let output = tg build $path
snapshot $output
