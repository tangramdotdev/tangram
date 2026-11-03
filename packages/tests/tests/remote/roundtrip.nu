use ../../util/test.nu *

# A remote can be put, retrieved, listed, and deleted.

let local = server spawn

tg remote put upstream "http://localhost:9999"

let got = tg remote get upstream | from json
assert ($got.id | str starts-with "rmt_") "a remote should have a remote ID"
assert equal ($got | reject id) { name: "upstream", trusted: false, url: "http://localhost:9999" } "the remote should round trip"
let by_id = tg remote get $got.id | from json
assert equal $by_id $got "the remote should be found by its ID"

let list = tg remote list | from json
assert equal ($list | get node) [$got.id] "the remote should be listed"
let list = tg remote list --expand | from json
assert equal ($list | get data) [$got] "the expanded remote should be listed"

# Put updates a remote selected by its ID.
tg remote put $got.id "http://localhost:9998"
assert equal (tg remote get upstream | from json | get url) "http://localhost:9998" "the remote should be updated by its ID"
failure (tg remote put "" "http://localhost:9999" | complete) "a remote name must not be empty"

let output = tg remote delete $got.id | complete
success $output

# Put does not create a remote for an unknown ID.
let output = tg remote put $got.id "http://localhost:9999" | complete
failure $output "putting an unknown remote ID should fail"
assert ($output.stderr | str contains "failed to find the remote") "putting an unknown remote ID should report a missing remote"

let output = tg remote get upstream | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to find the remote
	   remote = upstream

'
