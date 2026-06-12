use ../../../test.nu *

# A directory's tryGet method returns undefined when the path does not exist.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let directory = await tg.directory({ "a": "alpha" });
			return (await directory.tryGet("nope")) === undefined;
		};
	'
}

let output = tg build $path
snapshot $output 'true'
