use ../lib/test.nu *

# A thrown JavaScript error should be indexed without an authorization search.

let server = server spawn --config {
	verification: { permissions: { final: false, initial: false } }
}
let path = artifact {
	tangram.ts: '
		export default function () { throw new Error("boom"); }
		export function tangram() { throw tg.error.sync("boom"); }
	'
}
for reference in [$path $'($path)#tangram'] {
	let id = tg build --detach $reference
	let outcome = tg wait $id | from json
	assert ((tg get $outcome.error) | str contains '"message":"boom"')
	tg index
	let process = tg process get --source index $id | from json
	assert equal $process.status finished "the failed process should be finished in the index"
}
