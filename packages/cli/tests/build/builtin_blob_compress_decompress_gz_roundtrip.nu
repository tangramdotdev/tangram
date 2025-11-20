use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let blob = await tg.blob("contents");
			let compressed = await tg.compress(blob, "gz");
			let decompressed = await tg.decompress(compressed, "gz");
			return blob.text();
		};
	'
}

let output = tg build $path | complete
success $output
snapshot $output.stdout
