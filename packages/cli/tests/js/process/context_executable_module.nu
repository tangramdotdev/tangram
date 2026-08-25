use ../../../test.nu *

# The ambient tg.process.module carries the module that is being built.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return tg.process.module instanceof tg.Module;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
