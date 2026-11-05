use ../../util/test.nu *

# A creator can create separate grants for the same subject and resource, and a permission lasts while any of them holds it.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

tg --token $alice.token group create team

# Creating a grant with the create option makes a separate grant.
let first = tg --token $alice.token grant $bob.user.id read team | from json
let second = tg --token $alice.token grant --create $bob.user.id read team | from json
assert ($first.id != $second.id) "creating a grant should make a separate grant"

# Granting without the create option adds to the oldest grant.
let updated = tg --token $alice.token grant $bob.user.id write team | from json
assert equal $updated.id $first.id "granting should add to the oldest grant"

# Deleting one grant keeps the permissions that another grant still holds.
tg --token $alice.token grants delete $first.id
tg index --database
success (tg --token $bob.token group get team | complete) "the remaining grant should keep the subject's access"

# Revoking removes the permission from every matching grant and deletes the grants that have none left.
tg --token $alice.token revoke $bob.user.id read team
tg index --database
failure (tg --token $bob.token group get team | complete) "revoking should remove the subject's access"
failure (tg --token $alice.token grants get $second.id | complete) "a grant with no permissions left should be deleted"
