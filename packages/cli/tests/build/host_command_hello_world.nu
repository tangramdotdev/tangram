use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			return await tg.run`echo "Hello, World!"`;
		};
	'
}

run tg build $path
