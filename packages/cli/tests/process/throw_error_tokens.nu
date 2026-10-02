use ../lib/test.nu *

# A thrown JavaScript error should be indexed when final authorization searches are disabled.

let server = server spawn --config {
	authorization: { final: false }
}

let path = artifact {
	tangram.ts: 'export default function () { throw new Error("boom"); }'
}
let id = tg build --detach $path
let output = tg wait $id | from json
assert ((tg get $output.error) | str contains '"message":"boom"') "the process should fail with the thrown error"
tg index
let process = tg process get --source index $id | from json
assert equal $process.status finished "the failed process should be finished in the index"
