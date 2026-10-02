use ../lib/test.nu *

# With authentication disabled, the root principal may create a sandbox owned by root.

let local = server spawn

let sandbox = tg sandbox create --no-tokens --owner root --no-network | referent node
let data = tg sandbox get $sandbox | from json | get data
assert equal $data.owner "root" "a root principal should create a root-owned sandbox when authentication is disabled"

tg sandbox destroy $sandbox
