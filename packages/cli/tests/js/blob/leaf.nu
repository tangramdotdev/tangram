use ../../../test.nu *

# tg.Blob.leaf concatenates its arguments into a single leaf node holding bytes.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let blob = await tg.Blob.leaf("aa", "bb");
			return [await blob.text, "bytes" in (await blob.object())];
		}
	'
}

let output = tg build $path
snapshot $output '["aabb",true]'
