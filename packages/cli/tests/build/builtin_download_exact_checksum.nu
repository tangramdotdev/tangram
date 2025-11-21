use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let blob = await tg.download("https://www.example.com", "sha256:6f5635035f36ad500b4fc4bb7816bb72ef5594e1bcae44fa074c5e988fc4c0fe");
			return tg.file(blob);
		};
	'
}

let output = run tg build $path
snapshot $output
