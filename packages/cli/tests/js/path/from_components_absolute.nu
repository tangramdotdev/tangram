use ../../../test.nu *

# tg.path.fromComponents reconstructs an absolute path from a leading root component.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.path.fromComponents(["/", "a", "b"]); }'
}

let output = tg build $path
snapshot $output '"/a/b"'
