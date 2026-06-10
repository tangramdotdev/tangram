use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let blob = await tg.download("http://www.example.com", "sha256:ff67a9d764d6a2367a187734e697f6a53217db9a21c101d410a113ca871a299d");
			return tg.file(blob);
		};
	'
}

let output = tg build $path
snapshot $output
