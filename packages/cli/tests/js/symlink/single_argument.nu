use ../../../test.nu *

# tg.symlink preserves an unloaded symlink passed as its sole argument without loading it.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let input = await tg.symlink("target");
			await input.store();
			input.unload();
			let output = await tg.symlink(input);
			return input === output && input.state.object === null;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
