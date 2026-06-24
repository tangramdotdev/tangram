use ../../../test.nu *

# A directory's tryGet method returns the artifact at the given path.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory({ "a": "alpha" });
			let file = await directory.tryGet("a");
			return await (file as tg.File).text;
		}
	'
}

let output = tg build $path
snapshot $output '"alpha"'
