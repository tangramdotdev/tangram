use ../../test.nu *

# Listing an organization's members returns every member.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let carol = tg login --verbose carol | from json

tg --token $alice.token organization create acme
tg --token $alice.token organization members add acme $bob.user.id
tg --token $alice.token organization members add acme $carol.user.id

let members = tg --token $alice.token organization members list acme | from json
assert ($bob.user.id in $members) "bob should be listed as a member"
assert ($carol.user.id in $members) "carol should be listed as a member"
assert (($members | length) == 2) "the organization should have exactly two members"
