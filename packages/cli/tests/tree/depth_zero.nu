use ../../test.nu *

# Displaying a tree with depth zero renders only the root.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return 42; }'
}
tg tag root $path

let output = tg tree root --depth 0
snapshot --normalize-ids $output 'root: dir_010000000000000000000000000000000000000000000000000000'
