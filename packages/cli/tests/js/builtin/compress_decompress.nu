use ../../../test.nu *

# tg.compress and tg.decompress round-trip a blob through every supported format.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let formats: Array<tg.CompressionFormat> = ["bz2", "gz", "xz", "zst"];
			let results = [];
			for (let format of formats) {
				let blob = await tg.blob("compress me please");
				let compressed = await tg.compress(blob, format);
				let decompressed = await tg.decompress(compressed);
				results.push(await decompressed.text);
			}
			return results;
		}
	'
}

let output = tg build $path
snapshot $output '["compress me please","compress me please","compress me please","compress me please"]'
