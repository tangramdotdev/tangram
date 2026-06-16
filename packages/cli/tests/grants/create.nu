use ../../test.nu *

# Creating a grant returns the grant record and lists it on the resource.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team

let grant = tg --token $alice.token grant $bob.user.id read team | from json
assert ($grant.permission == "read") "the grant should record the permission"
assert ($grant.principal == $bob.user.id) "the grant should record the principal"

let grants = tg --token $alice.token grants list --resource team | from json
assert ($grants | any {|g| $g.principal == $bob.user.id and $g.permission == "read" }) "the grant should be listed on the resource"
