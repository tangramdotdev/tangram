use ../../test.nu *

# The depth flag limits the tree to the given number of levels below the root.

let server = server spawn

let path = artifact {
	tangram.ts: 'export default function () { return 42; }'
}
tg tag root $path

let output = tg tree root --depth 1
snapshot --normalize-ids $output '
	root
	└╴target: dir_010000000000000000000000000000000000000000000000000000
'
