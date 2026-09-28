use ../lib/test.nu *

# tg.download succeeds when the downloaded contents match the exact sha256 checksum that was provided.

skip_if_offline

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let blob = await tg.download("http://www.example.com", "sha256:7d3e61f8f627c8cc640c209ba0777db95f198a64e766f47484c321d5eb5e0962");
			return tg.file(blob);
		}
	'
}

let output = tg build --no-tokens $path
snapshot $output
