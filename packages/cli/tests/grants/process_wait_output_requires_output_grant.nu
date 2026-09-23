use ../lib/test.nu *

# Waiting returns an output object reference; reading the object requires separate authorization.

def assert_output [output: record] {
	snapshot --normalize-ids ($output | update value {|output| $output.value | split row '?' | first } | to json) '
		{
		  "kind": "object",
		  "value": "fil_010000000000000000000000000000000000000000000000000000"
		}
	'
}

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

# Alice builds a process whose output is a file.
let path = artifact { tangram.ts: 'export default async function () { return tg.file("secret"); }' }
let process = tg --token $alice.token build --detach $path | str trim

# The owner can read the output from either the live or stored wait response.
let aliceresult = tg --token $alice.token wait $process | from json
assert ($aliceresult.exit == 0) "the build should succeed."
assert_output $aliceresult.output
assert equal (tg --token $alice.token cat $aliceresult.output.value | str trim) secret

# Alice grants Eve only process_node (basic read), not the output permission.
tg --token $alice.token grant $eve.user.id process_node $process

# Eve sees the process finished and the output object id, but receives no capability for it.
let everesult = tg --token $eve.token wait $process | from json
assert ($everesult.exit == 0) "Eve should see the process exit."
assert_output $everesult.output
let params = $'http://localhost/($everesult.output.value)' | url parse | get params
assert ($params | is-empty) "a process node grant must not expose output capabilities."
failure (tg --token $eve.token cat $everesult.output.value | complete) "a node grant must not authorize reading the output"

# Granting Eve the output permission authorizes the object.
tg --token $alice.token grant $eve.user.id process_node_output $process
let everesult2 = tg --token $eve.token wait $process | from json
assert_output $everesult2.output
assert equal (tg --token $eve.token cat $everesult2.output.value | str trim) secret

# The stored process and its wait response contain no capabilities.
tg --token $alice.token index
let data = tg --token $eve.token get $process | from json
assert equal $data.output.value ($aliceresult.output.value | split row '?' | first)
assert equal ($everesult2.output.value | split row '?' | first) $data.output.value
