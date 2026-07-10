use ../../test.nu *

# The public resolver must reject undefined at compile time.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			if (false) {
				// @ts-expect-error Undefined is not a Tangram value.
				await tg.resolve(undefined);
			}
			return true;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
