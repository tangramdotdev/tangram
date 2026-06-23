use ../../test.nu *

# A process's output value is revealed only to a principal with the output permission; a process_node grant alone masks the output on both the wait and get paths, and only an authorized reader receives the output entitlement token.

let server = spawn --config { authentication: true }
let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a process whose output is a file.
let path = artifact { tangram.ts: 'export default async function () { return tg.file("secret"); }' }
let process = tg --token $alice.token build --detach $path | str trim

# The owner reads the output and receives an entitlement token for the output object.
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

# Eve sees the process finished, but the output value is masked on the wait path.
let everesult = tg --token $eve.token wait $process | from json
assert ($everesult.exit == 0) "Eve should see the process exit."
assert ($everesult.output? == null) "a process_node grant alone must mask the output on the wait path."

# The output value is also masked on the get path, so the mask is not specific to wait.
let eveget = tg --token $eve.token get $process | from json
assert ($eveget.output? == null) "a process_node grant alone must mask the output on the get path."

# Granting Eve the output permission reveals the value and yields the token on wait.
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
