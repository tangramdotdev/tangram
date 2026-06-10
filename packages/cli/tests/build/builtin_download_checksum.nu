use ../../test.nu *

# tg.download succeeds when given the wildcard "sha256:any" checksum, which accepts any downloaded contents.

skip_if_offline

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let blob = await tg.download("http://www.example.com", "sha256:any");
			return tg.file(blob);
		};
	'
}

let output = tg build $path
snapshot $output
