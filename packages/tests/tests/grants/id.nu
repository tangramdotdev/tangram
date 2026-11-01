use ../../util/test.nu *

# A grant has an ID that its creator can use to get and delete it, and regranting keeps the ID.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let eve = tg login --verbose --name eve | from json

tg --token $alice.token group create team

# Creating a grant returns its ID, and granting more to the same subject keeps the ID.
let grant = tg --token $alice.token grant $bob.user.id read team | from json
assert ($grant.id | str starts-with "grt_") "a grant should have a grant ID"
let updated = tg --token $alice.token grant $bob.user.id write team | from json
assert equal $updated.id $grant.id "granting more permissions should keep the grant's ID"

# The grant's creator can get it by its ID, and the list returns its ID.
let got = tg --token $alice.token grants get $grant.id | from json
assert equal $got.id $grant.id
assert equal $got.creator $alice.user.id
assert equal $got.permissions { kind: group, value: [read write] }
let grants = tg --token $alice.token grants list --resource team | from json
assert ($grant.id in ($grants | get node)) "the grant should be listed by its ID"
let listed = $grants | where node == $grant.id | first
let got = tg --token $alice.token grants get $"($listed.node)?location=($listed.options.location)" | from json
assert equal $got.id $grant.id "the grant should be found from its listed referent"

# The grant's subject can get the grant that it lists, but cannot delete it.
let listed = tg --token $bob.token grants list --subject $bob.user.id | from json
assert ($grant.id in ($listed | get node)) "the subject should list the grant by its ID"
let got = tg --token $bob.token grants get $grant.id | from json
assert equal $got.id $grant.id "the subject should get the grant that it lists"
failure (tg --token $bob.token grants delete $grant.id | complete) "the subject must not delete the grant"

# Another user cannot get or revoke the grant.
failure (tg --token $eve.token grants get $grant.id | complete) "another user must not get the grant"
failure (tg --token $eve.token grants delete $grant.id | complete) "another user must not revoke the grant"

# A creator who is no longer an admin of the resource can still delete its grant.
tg --token $alice.token grant $eve.user.id admin team
let eve_grant = tg --token $eve.token grant $bob.user.id read team | from json
tg --token $alice.token revoke $eve.user.id admin team
tg index
tg --token $eve.token grants delete $eve_grant.id
failure (tg --token $alice.token grants get $eve_grant.id | complete) "the deleted grant should not be found"

# The creator can revoke the grant by its ID.
tg --token $alice.token grants delete $grant.id
failure (tg --token $alice.token grants get $grant.id | complete) "the revoked grant should not be found"
let grants = tg --token $alice.token grants list --resource team | from json
assert ($grant.id not-in ($grants | get node)) "the revoked grant should not be listed"

# Deleting a grant that no longer exists fails.
let output = tg --token $alice.token grants delete $grant.id | complete
failure $output "deleting a missing grant should fail"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to find the grant
	   grant = grt_0000000000000000000000000000

'
