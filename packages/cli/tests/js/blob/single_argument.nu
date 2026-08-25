use ../../../test.nu *

# tg.blob preserves an unloaded blob passed as its sole argument without loading it.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let input = await tg.blob("contents");
			await input.store();
			input.unload();
			let output = await tg.blob(input);
			return input === output && input.state.object === null;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
