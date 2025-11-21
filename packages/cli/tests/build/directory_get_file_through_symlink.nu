use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let directory = await tg.directory({
				"subdirectory": {
					"hello.txt": "Hello, World!",
				},
				"link": tg.symlink("subdirectory"),
			});
			return directory.get("link/hello.txt");
		};
	'
}

# Build.
let output = run tg build $path
snapshot $output
