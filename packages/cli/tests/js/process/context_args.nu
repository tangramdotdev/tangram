use ../../../test.nu *

# The ambient tg.process.args is an empty list for a default-export build with no arguments.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return tg.process.args;
		}
	'
}

let output = tg build $path
snapshot $output '[]'
