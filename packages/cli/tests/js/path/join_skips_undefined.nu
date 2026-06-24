use ../../../test.nu *

# tg.path.join ignores undefined arguments.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.path.join("a", undefined, "b"); }'
}

let output = tg build $path
snapshot $output '"a/b"'
