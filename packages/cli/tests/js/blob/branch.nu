use ../../../test.nu *

# tg.Blob.branch builds a branch node whose children concatenate to the contents.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let blob = await tg.Blob.branch("aa", "bb");
			return [await blob.text, "children" in (await blob.object())];
		};
	'
}

let output = tg build $path
snapshot $output '["aabb",true]'
