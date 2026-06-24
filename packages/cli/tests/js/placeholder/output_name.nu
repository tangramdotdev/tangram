use ../../../test.nu *

# The built-in output placeholder is named output.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.output.name; }'
}

let output = tg build $path
snapshot $output '"output"'
