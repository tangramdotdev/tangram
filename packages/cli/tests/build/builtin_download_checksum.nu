use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let blob = await tg.download("https://www.example.com", "sha256:any");
			return tg.file(blob);
		};
	'
}

let output = run tg build $path
snapshot $output
