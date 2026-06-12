use ../../../test.nu *

# tg.Blob.withId returns a blob that preserves the given id.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let blob = await tg.blob("hello");
			return tg.Blob.withId(blob.id).id === blob.id;
		};
	'
}

let output = tg build $path
snapshot $output 'true'
