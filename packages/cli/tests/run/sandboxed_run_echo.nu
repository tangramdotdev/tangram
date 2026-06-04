use ../../test.nu *

# A sandboxed tg.run template command's stdout is inherited and captured on the parent run's stdout.

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
snapshot $output.stdout '
	hello

'
