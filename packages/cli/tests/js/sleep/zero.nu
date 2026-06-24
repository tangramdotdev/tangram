use ../../../test.nu *

# tg.sleep with a zero duration resolves immediately.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.sleep(0);
			return "ok";
		}
	'
}

let output = tg build $path
snapshot $output '"ok"'
