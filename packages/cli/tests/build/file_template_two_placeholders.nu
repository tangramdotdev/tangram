use ../../test.nu *

# A tg.file template literal interpolates a string placeholder into a multiline file and the resulting text matches the snapshot.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			let a = "string!";
			return tg.file`\n\tHello, World!\n\t${a}\n`.then((f) => f.text);
		}
	'
}

let output = tg build $path
snapshot $output
