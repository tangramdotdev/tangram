use ../../../test.nu *

# tg.path.parent returns a relative path with its last component removed.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.path.parent("a/b/c"); }'
}

let output = tg build $path
snapshot $output '"a/b"'
