use ../../test.nu *

# Compressing a blob with gzip and then decompressing it preserves the original contents.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let blob = await tg.blob("contents");
			let compressed = await tg.compress(blob, "gz");
			let decompressed = await tg.decompress(compressed, "gz");
			return blob.text;
		}
	'
}

let output = tg build $path
snapshot $output
