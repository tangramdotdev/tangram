use ../../lib/test.nu *

# tg.path.fromComponents joins components without a root into a relative path.

let local = server spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.path.fromComponents(["a", "b", "c"]); }'
}

let output = tg build $path
snapshot $output '"a/b/c"'
