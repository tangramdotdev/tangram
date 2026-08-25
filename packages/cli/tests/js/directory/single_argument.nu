use ../../../test.nu *

# tg.directory preserves a directory passed as its sole argument.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let input = await tg.directory({ "file": "contents" });
			await input.store();
			input.unload();
			let output = await tg.directory(input);
			return input === output && input.state.object === null;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
