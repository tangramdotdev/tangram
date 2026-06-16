use ../../test.nu *

# The permission lattice is admin implies write implies read, and the implication does not run the other way.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let carol = tg login --verbose carol | from json
let dave = tg login --verbose dave | from json

tg --token $alice.token group create team

# Admin implies read and the admin-only operation of listing grants.
tg --token $alice.token grant $bob.user.id admin team
tg --token $bob.token group get team
tg --token $bob.token grants list --resource team

# Write implies read, so a write user can read the group.
tg --token $alice.token grant $dave.user.id write team
tg --token $dave.token group get team
# Write allows the write operation of creating a subgroup.
tg --token $dave.token group create team/dave-sub
# Write does not imply admin, so a write user cannot list grants.
let dave_output = tg --token $dave.token grants list --resource team | complete
failure $dave_output "write permission should not allow listing grants"
assert ($dave_output.stderr | str contains "unauthorized") "the error should mention that the request is unauthorized"

# Read allows reading but not the write operation of creating a subgroup.
tg --token $alice.token grant $carol.user.id read team
tg --token $carol.token group get team
let carol_write = tg --token $carol.token group create team/carol-sub | complete
failure $carol_write "read permission should not allow creating a subgroup"
assert ($carol_write.stderr | str contains "unauthorized") "the error should mention that the request is unauthorized"
# Read does not imply admin, so a read user cannot list grants.
let carol_output = tg --token $carol.token grants list --resource team | complete
failure $carol_output "read permission should not allow listing grants"
assert ($carol_output.stderr | str contains "unauthorized") "the error should mention that the request is unauthorized"
