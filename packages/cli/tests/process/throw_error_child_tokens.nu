use ../lib/test.nu *

# Wrapping a failed child retains its error proofs along with the parent module proof.

let server = server spawn --config {
	verification: { permissions: { final: false, initial: false } }
}
let path = artifact {
	tangram.ts: '
		export default async function () { await tg.build(child); }
		export function child() { throw new Error("boom"); }
	'
}
let id = tg build --detach $path
let outcome = tg wait $id | from json
assert ((tg get $outcome.error) | str contains '"message":"the child process failed"')
tg index
let process = tg process get --source index $id | from json
assert equal $process.status finished "the parent error should be indexed"
