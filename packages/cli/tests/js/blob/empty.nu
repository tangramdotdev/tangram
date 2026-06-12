use ../../../test.nu *

# tg.blob with no argument creates an empty blob.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let blob = await tg.blob();
			return [await blob.length, await blob.text];
		};
	'
}

let output = tg build $path
snapshot $output '[0,""]'
