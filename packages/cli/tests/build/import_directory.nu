use ../../test.nu *

# A module can import a directory with a type attribute and read a file from within it.

let server = spawn

let path = artifact {
	tangram.ts: '
		import directory from "./directory" with { type: "directory" };
		export default async function () { return directory.get("hello.txt")
				.then(tg.File.expect)
				.then((f) => f.text); }
	'
	directory: {
		hello.txt: 'Hello, World!'
	}
}

let output = tg build $path
snapshot $output
