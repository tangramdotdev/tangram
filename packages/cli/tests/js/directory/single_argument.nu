use ../../../test.nu *

# tg.directory preserves a directory passed as its sole argument.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let input = await tg.directory({ "file": "contents" });
			let output = await tg.directory(input);
			return input === output;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
