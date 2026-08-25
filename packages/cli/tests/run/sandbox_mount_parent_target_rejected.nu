use ../../test.nu *

# A sandbox mount target cannot traverse above the guest root.

let server = server spawn

let source = mktemp --directory | str trim
let path = artifact {
	tangram.ts: 'export default async function () { await tg.run`true`.sandbox(); }',
}
let output = tg run --sandbox --mount $'($source):/../escape' $path | complete
failure $output
assert ($output.stderr | str contains 'mount targets may not contain parent directory components')
