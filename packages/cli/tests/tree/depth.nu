use ../../test.nu *

# The depth flag limits the tree to the given number of levels below the root.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return 42; }'
}
tg tag root $path

let output = tg tree root --depth 1
snapshot ($output | normalize_ids) '
	root: dir_010000000000000000000000000000000000000000000000000000
	└╴entries: map
'
