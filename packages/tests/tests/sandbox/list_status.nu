use ../../util/test.nu *

# tg sandbox list lists started sandboxes by default, and a destroyed sandbox remains listed with the destroyed status.

let local = server spawn

let sandbox = tg sandbox create --no-tokens --no-network | referent node
tg index

# The started sandbox is listed by default.
let started = tg sandbox list | from json | get node
assert ($sandbox in $started) "a started sandbox should be listed by default"

# Destroy the sandbox.
tg sandbox destroy $sandbox
tg wait $sandbox | ignore
tg index

# The destroyed sandbox is listed only with the destroyed status or any status.
let started = tg sandbox list | from json | get node
assert ($sandbox not-in $started) "a destroyed sandbox should not be listed by default"
let destroyed = tg sandbox list --status destroyed --expand | from json
let listed = $destroyed | where data.id == $sandbox
assert equal ($listed | length) 1 "a destroyed sandbox should be listed with the destroyed status"
assert equal ($listed | first | get data.status) destroyed
let any = tg sandbox list --any-status | from json | get node
assert ($sandbox in $any) "a destroyed sandbox should be listed with any status"
