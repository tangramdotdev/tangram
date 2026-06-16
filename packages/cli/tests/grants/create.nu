use ../../test.nu *

# Creating a grant returns the grant record and lists it on the resource.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team

let grant = tg --token $alice.token grant $bob.user.id read team | from json
assert ($grant.permissions == "read") "the grant should record the permissions"
assert ($grant.principal == $bob.user.id) "the grant should record the principal"

let grants = tg --token $alice.token grants list --resource team | from json
assert ($grants | any {|g| $g.principal == $bob.user.id and $g.permissions == "read" }) "the grant should be listed on the resource"

let grant = tg --token $alice.token grant $bob.user.id write team | from json
assert ($grant.permissions == "read,write") "granting another permission should update the creator's grant"

let grants = tg --token $alice.token grants list --resource team | from json
assert ($grants | any {|g| $g.principal == $bob.user.id and $g.permissions == "read,write" }) "the updated grant should be listed as a permission set"
