use ../../../test.nu *

# tg.command preserves an unloaded command passed as its sole argument without loading it.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let input = await tg.command({ executable: "echo", host: tg.host.current });
			await input.store();
			input.unload();
			let output = await tg.command(input);
			return input === output && input.state.object === null;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
