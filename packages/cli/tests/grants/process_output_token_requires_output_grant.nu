use ../../test.nu *

# A process's output entitlement token is minted only for a principal with the output permission; a process_node grant reveals the output object id but withholds the token that grants access to its content.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a process whose output is a file.
let path = artifact { tangram.ts: 'export default async function () { return tg.file("secret"); }' }
let process = tg --token $alice.token build --detach $path | str trim

# The owner receives the output object together with an entitlement token for it.
let aliceresult = tg --token $alice.token wait $process | from json
assert ($aliceresult.exit == 0) "the build should succeed."
snapshot ($aliceresult.output? | to json | normalize_ids) '
	{
	  "kind": "object",
	  "value": {
	    "id": "fil_010000000000000000000000000000000000000000000000000000",
	    "token": "<token>"
	  }
	}
'

# Alice grants Eve only process_node (basic read), not the output permission.
tg --token $alice.token grant $eve.user.id process_node $process

# Eve sees the process finished and the output object id, but receives no entitlement token for it.
let everesult = tg --token $eve.token wait $process | from json
assert ($everesult.exit == 0) "Eve should see the process exit."
snapshot ($everesult.output? | to json | normalize_ids) '
	{
	  "kind": "object",
	  "value": "fil_010000000000000000000000000000000000000000000000000000"
	}
'

# Granting Eve the output permission yields the token.
tg --token $alice.token grant $eve.user.id process_node_output $process
let everesult2 = tg --token $eve.token wait $process | from json
snapshot ($everesult2.output? | to json | normalize_ids) '
	{
	  "kind": "object",
	  "value": {
	    "id": "fil_010000000000000000000000000000000000000000000000000000",
	    "token": "<token>"
	  }
	}
'
