use ../../test.nu *

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
let output = tg build $path | complete
success $output

snapshot $output.stdout
