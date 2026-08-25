use ../../../test.nu *

# tg.graph preserves an unloaded graph passed as its sole argument without loading it.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let input = await tg.graph({ nodes: [{ kind: "file", contents: "contents" }] });
			await input.store();
			input.unload();
			let output = await tg.graph(input);
			return input === output && input.state.object === null;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
