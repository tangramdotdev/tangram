use ../../test.nu *

# Putting bytes under an id they do not hash to is rejected by the server, and the object is not stored.

let server = spawn

let real = tg put 'tg.file("real")' | str trim
let bytes = tg get $real --bytes
let fake = "fil_010000000000000000000000000000000000000000000000000000"

let output = $bytes | tg object put $fake --bytes | complete
failure $output
snapshot --normalize-ids $output.stderr '
	error an error occurred
	-> failed to put the object
	   id = fil_010000000000000000000000000000000000000000000000000000
	-> the request failed
	   status = 400 Bad Request
	-> invalid object id
	   actual = fil_011111111111111111111111111111111111111111111111111111
	   expected = fil_010000000000000000000000000000000000000000000000000000

'

let output = tg get $fake | complete
failure $output
