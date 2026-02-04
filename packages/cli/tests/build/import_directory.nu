use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		import directory from "./directory" with { type: "directory" };
		export default async () =>
			directory.get("hello.txt")
				.then(tg.File.expect)
				.then((f) => f.text)
		;
	'
	directory: {
		hello.txt: 'Hello, World!'
	}
}

let output = tg build $path
snapshot $output
