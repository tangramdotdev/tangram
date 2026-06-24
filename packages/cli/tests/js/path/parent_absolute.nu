use ../../../test.nu *

# tg.path.parent removes the last component of an absolute path without doubling the leading slash.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.path.parent("/a/b"); }'
}

let output = tg build $path
snapshot $output '"/a"'
