use ../../test.nu *

# Listing sandboxes must be scoped to the principal: a principal must not see sandboxes created by another principal. Eve's list does not include Alice's sandbox, while Alice's own list does.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice creates a sandbox.
let sandbox = tg --token $alice.token sandbox create --no-network | str trim

# Alice sees her own sandbox.
let alice_list = tg --token $alice.token sandbox list | from json
assert (($alice_list | where id == $sandbox | is-empty) == false) "Alice should see her own sandbox."

# Eve must not see Alice's sandbox.
let eve_list = tg --token $eve.token sandbox list | from json
assert ($eve_list | where id == $sandbox | is-empty) "Eve must not see a sandbox she did not create."
