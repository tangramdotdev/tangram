use ../../test.nu *

# A principal without a grant cannot signal another principal's private process; the process is masked as not found rather than reporting that it exists.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a private process.
let path = artifact { tangram.ts: 'export default function () { return 5; }' }
let process = tg --token $alice.token build --detach $path | str trim
tg --token $alice.token wait $process

# Eve must not be able to signal a process she cannot see, and the error must not reveal that it exists.
let signaled = tg --token $eve.token process signal $process | complete
failure $signaled ("Eve must not signal a process she cannot see: " + ($signaled | to json))
snapshot --normalize $signaled.stderr '
	error an error occurred
	-> failed to signal the process
	   id = pcs_0000000000000000000000000000
	-> failed to find the process

'
