use ../../test.nu *

# Listing a resource's grants returns every grant on it, including the creator's admin grant.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team
tg --token $alice.token grant $bob.user.id read team

let grants = tg --token $alice.token grants list --resource team | from json
assert ($grants | any {|g| $g.principal == $alice.user.id and $g.permissions == "group_admin" }) "the creator's admin grant should be listed"
assert ($grants | any {|g| $g.principal == $bob.user.id and $g.permissions == "group_read" }) "bob's read grant should be listed"
