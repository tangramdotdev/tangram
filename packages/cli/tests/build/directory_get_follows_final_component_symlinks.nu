use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		import directory from "./directory" with { type: "directory" };
		export default async () => {
			let file = await directory.get("link");
			tg.File.assert(file);
			return file.text();
		};
	'
	directory: (directory {
		hello.txt: 'foo'
		link: (symlink 'hello.txt')
	})
}

let output = run tg build $path
snapshot $output
