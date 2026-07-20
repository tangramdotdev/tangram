use ../../test.nu *

# A remote can be put, retrieved, listed, and deleted.

let server = spawn

tg remote put upstream "http://localhost:9999"

let got = tg remote get upstream | from json
assert equal $got { name: "upstream", url: "http://localhost:9999" } "the remote should round trip"

let list = tg remote list | from json
assert equal $list [{ name: "upstream", url: "http://localhost:9999" }] "the remote should be listed"

let output = tg remote delete upstream | complete
success $output

let output = tg remote get upstream | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to find the remote
	   name = upstream

'
