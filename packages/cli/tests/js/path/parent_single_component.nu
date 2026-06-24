use ../../../test.nu *

# tg.path.parent returns an empty path when the input has a single component.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.path.parent("a"); }'
}

let output = tg build $path
snapshot $output '""'
