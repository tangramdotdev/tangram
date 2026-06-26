use ../../test.nu *

# Deleting a grant removes it, and revoke is an alias for delete.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team

# grants delete removes a grant.
tg --token $alice.token grant $bob.user.id read team
tg --token $alice.token grants delete $bob.user.id read team
let grants = tg --token $alice.token grants list --resource team | from json
assert (not ($grants | any {|g| $g.principal == $bob.user.id and $g.permissions == "group_read" })) "the grant should be gone after delete"

# revoke is an alias for grants delete.
tg --token $alice.token grant $bob.user.id write team
tg --token $alice.token revoke $bob.user.id write team
let grants = tg --token $alice.token grants list --resource team | from json
assert (not ($grants | any {|g| $g.principal == $bob.user.id and $g.permissions == "group_write" })) "revoke should remove the grant"
