use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.run`echo hello`.sandbox();
		}
	',
}

let output = tg run $path | complete
success $output
