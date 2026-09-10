use ../../test.nu *

# Waiting returns an output object reference; reading the object requires separate authorization.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

# Alice builds a process whose output is a file.
let path = artifact { tangram.ts: 'export default async function () { return tg.file("secret"); }' }
let process = tg --token $alice.token build --detach $path | str trim

# The owner receives the output object reference without an entitlement token.
let aliceresult = tg --token $alice.token wait $process | from json
assert ($aliceresult.exit == 0) "the build should succeed."
snapshot --normalize-ids ($aliceresult.output? | to json) '
	{
	  "kind": "object",
	  "value": "fil_010000000000000000000000000000000000000000000000000000"
	}
'
assert equal (tg --token $alice.token cat $aliceresult.output.value | str trim) secret

# Alice grants Eve only process_node (basic read), not the output permission.
tg --token $alice.token grant $eve.user.id process_node $process

# Eve sees the process finished and the output object id, but receives no entitlement token for it.
let everesult = tg --token $eve.token wait $process | from json
assert ($everesult.exit == 0) "Eve should see the process exit."
snapshot --normalize-ids ($everesult.output? | to json) '
	{
	  "kind": "object",
	  "value": "fil_010000000000000000000000000000000000000000000000000000"
	}
'
failure (tg --token $eve.token cat $everesult.output.value | complete) "a node grant must not authorize reading the output"

# Granting Eve the output permission authorizes reading the object without minting a token in wait.
tg --token $alice.token grant $eve.user.id process_node_output $process
let everesult2 = tg --token $eve.token wait $process | from json
snapshot --normalize-ids ($everesult2.output? | to json) '
	{
	  "kind": "object",
	  "value": "fil_010000000000000000000000000000000000000000000000000000"
	}
'
assert equal (tg --token $eve.token cat $everesult2.output.value | str trim) secret
