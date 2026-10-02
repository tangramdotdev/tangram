use ../lib/test.nu *

# Signalling a cacheable process fails because cacheable processes cannot receive signals.

let local = server spawn

let path = artifact {
	tangram.ts: 'export default async function () { return 42; }',
}
let process = tg build --no-tokens --detach $path | referent node
tg wait $process

let output = tg signal $process | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to signal the process
	   id = pcs_0000000000000000000000000000
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to post process signal
	-> failed to signal the process
	   id = pcs_0000000000000000000000000000
	-> cannot signal cacheable processes
	   id = pcs_0000000000000000000000000000

'
