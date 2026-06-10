use ../../test.nu *

# Directory.get returns the symlink artifact itself when the requested entry is a symlink.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let directory = await tg.directory({
				"hello.txt": "Hello, World!",
				"link": tg.symlink("hello.txt"),
			});
			return directory.get("link");
		};
	'
}

# Build.
let output = tg build $path
snapshot $output
