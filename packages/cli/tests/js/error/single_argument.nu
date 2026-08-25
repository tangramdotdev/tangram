use ../../../test.nu *

# tg.error preserves an unloaded error passed as its sole argument without loading it.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let input = tg.error.sync("message", { stack: null });
			await input.store();
			input.unload();
			let output = await tg.error(input);
			return input === output && input.state.object === null;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
