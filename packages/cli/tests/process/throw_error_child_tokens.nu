use ../lib/test.nu *

# Wrapping a failed child retains the source error's proofs along with the parent's module proof.

let server = server spawn --config {
	authorization: { final: false, initial: false }
}
let path = artifact {
	tangram.ts: '
		export default async function () { await tg.build(child); }
		export function child() { throw new Error("boom"); }
	'
}
let id = tg build --detach $path
let output = tg wait $id | from json
assert ((tg get $output.error) | str contains '"message":"the child process failed"')
tg index
let process = tg process get --source index $id | from json
assert equal $process.status finished "the parent error should be indexed"
