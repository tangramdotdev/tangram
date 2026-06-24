use ../../test.nu *

# Directory.get resolves a path whose intermediate component is a symlink to a subdirectory and returns the target file.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory({
				"subdirectory": {
					"hello.txt": "Hello, World!",
				},
				"link": tg.symlink("subdirectory"),
			});
			return directory.get("link/hello.txt");
		}
	'
}

# Build.
let output = tg build $path
snapshot $output
