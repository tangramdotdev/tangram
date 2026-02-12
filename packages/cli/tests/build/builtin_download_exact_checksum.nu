use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let blob = await tg.download("https://www.example.com", "sha256:fb91d75a6bb430787a61b0aec5e374f580030f2878e1613eab5ca6310f7bbb9a");
			return tg.file(blob);
		};
	'
}

let output = tg build $path
snapshot $output
