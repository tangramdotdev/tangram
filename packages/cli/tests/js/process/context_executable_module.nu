use ../../../test.nu *

# The ambient tg.process.executable carries the module that is being built.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return "module" in tg.process.executable;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
