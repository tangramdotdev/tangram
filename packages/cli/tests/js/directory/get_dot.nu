use ../../../test.nu *

# A directory's get method ignores a leading current-directory component.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let directory = await tg.directory({ "a": "alpha" });
			let file = await directory.get("./a");
			return await (file as tg.File).text;
		};
	'
}

let output = tg build $path
snapshot $output '"alpha"'
