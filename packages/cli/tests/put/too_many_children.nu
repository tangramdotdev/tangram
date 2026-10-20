use ../lib/test.nu *

# An object with more than 1,024 children is rejected by the client, and by the server when it is sent directly.

let local = server spawn
let socket = $local.url | str replace 'http+unix://' '' | url decode

# Create a directory with 1,025 children.
let entries = 0..<1_025 | each { |i|
	let hash = $i | into string | fill --alignment right --character '0' --width 51
	{ name: ($i | into string), id: $'fil_01($hash)0' }
} | transpose --header-row --as-record
let data = { entries: $entries }

# The client rejects the object.
let output = $data | to json --raw | tg object put --no-tokens --bytes --kind directory | complete
failure $output 'the client must reject the object'
assert ($output.stderr | str contains 'the object has too many children') 'the client error must name the children limit'
assert not ($output.stderr | str contains 'the request failed') 'the client must reject the object before sending it'

# Get the object's ID from the server's response to a mismatched ID.
let body = { kind: 'directory', value: $data } | to json --raw
let response = http put --allow-errors --full --unix-socket $socket --content-type application/json 'http://localhost/objects/dir_010000000000000000000000000000000000000000000000000000' $body
let id = $response.body | from json | get values.actual

# The server rejects the object.
let response = http put --allow-errors --full --unix-socket $socket --content-type application/json $'http://localhost/objects/($id)' $body
assert equal $response.status 400 'the server must reject the object'
assert ($response.body | str contains 'the object has too many children') 'the server error must name the children limit'
let batch = { objects: [{ id: $id, data: ($body | from json) }] } | to json --raw
let response = http post --allow-errors --full --unix-socket $socket --content-type application/json 'http://localhost/objects/batch' $batch
assert equal $response.status 400 'the server must reject the object in a batch'
assert ($response.body | str contains 'the object has too many children') 'the server batch error must name the children limit'
let output = tg get $id | complete
failure $output 'the object must not be stored'
