use ../../../test.nu *

# tg.sleep resolves after the given duration, allowing the build to continue.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.sleep(0.05);
			return "resolved";
		}
	'
}

let output = tg build $path
snapshot $output '"resolved"'
