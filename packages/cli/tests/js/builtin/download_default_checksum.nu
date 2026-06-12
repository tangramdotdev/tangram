use ../../../test.nu *

# tg.download defaults to the wildcard "sha256:any" checksum when no checksum is given, returning a blob.

skip_if_offline

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let blob = await tg.download("http://www.example.com");
			return (blob instanceof tg.Blob) && (await blob.length) > 0;
		};
	'
}

let output = tg build $path
snapshot $output 'true'
