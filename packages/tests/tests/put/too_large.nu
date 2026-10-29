use ../../util/test.nu *

# An object with more than 1 MiB of data is rejected by the server, whether it is put with the CLI or sent directly.

let local = server spawn
let socket = $local.url | str replace 'http+unix://' '' | url decode

# Create a symlink with a 2 MiB path.
let data = { path: ('' | fill --character 'x' --width 2_097_152) }

# The server rejects the object put with the CLI.
let output = $data | to json --raw | tg object put --no-tokens --bytes --kind symlink | complete
failure $output 'the object put must fail'
assert ($output.stderr | str contains 'the object is too large') 'the error must name the size limit'

# Get the object's ID from the server's response to a mismatched ID.
let body = { kind: 'symlink', value: $data } | to json --raw
let response = http put --allow-errors --full --unix-socket $socket --content-type application/json 'http://localhost/objects/sym_010000000000000000000000000000000000000000000000000000' $body
let id = $response.body | from json | get values.actual

# The server rejects the object.
let response = http put --allow-errors --full --unix-socket $socket --content-type application/json $'http://localhost/objects/($id)' $body
assert equal $response.status 400 'the server must reject the object'
assert ($response.body | str contains 'the object is too large') 'the server error must name the size limit'
let batch = { objects: [{ id: $id, data: ($body | from json) }] } | to json --raw
let response = http post --allow-errors --full --unix-socket $socket --content-type application/json 'http://localhost/objects/batch' $batch
assert equal $response.status 400 'the server must reject the object in a batch'
assert ($response.body | str contains 'the object is too large') 'the server batch error must name the size limit'
let output = tg get $id | complete
failure $output 'the object must not be stored'
