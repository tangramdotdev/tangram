use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let blob = await tg.download("http://www.example.com", "nonsense");
			return tg.file(blob);
		};
	'
}

let output = tg build $path | complete
failure $output
