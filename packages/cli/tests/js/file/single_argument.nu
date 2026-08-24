use ../../../test.nu *

# tg.file preserves an unloaded file passed as its sole argument and does not load a sole blob contents argument.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let input = await tg.file("contents");
			await input.store();
			input.unload();
			let output = await tg.file(input);

			let blob = await tg.blob("contents");
			await blob.store();
			blob.unload();
			let file = await tg.file({ contents: blob });
			let contents = await file.contents;

			return (
				input === output &&
				input.state.object === null &&
				blob === contents &&
				blob.state.object === null
			);
		}
	'
}

let output = tg build $path
snapshot $output 'true'
