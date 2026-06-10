use ../../test.nu *

# tg.download fails when the downloaded contents do not match the provided sha256 checksum.

skip_if_offline

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let blob = await tg.download("http://www.example.com", "sha256:0000000000000000000000000000000000000000000000000000000000000000");
			return tg.file(blob);
		};
	'
}

let output = tg build $path | complete
failure $output
