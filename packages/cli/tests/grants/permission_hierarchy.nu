use ../../test.nu *

# The permission lattice is admin implies write implies read, and the implication does not run the other way.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let bob_user = tg user login bob | from json
let bob = current_token
let carol_user = tg user login carol | from json
let carol = current_token
let dave_user = tg user login dave | from json
let dave = current_token

tg --token $alice group create team

# Admin implies read and the admin-only operation of listing grants.
tg --token $alice grant $bob_user.id admin team
tg --token $bob group get team
tg --token $bob grants list --resource team

# Write implies read, so a write user can read the group.
tg --token $alice grant $dave_user.id write team
tg --token $dave group get team
# Write allows the write operation of creating a subgroup.
tg --token $dave group create team/dave-sub
# Write does not imply admin, so a write user cannot list grants.
let dave_output = tg --token $dave grants list --resource team | complete
failure $dave_output "write permission should not allow listing grants"
assert ($dave_output.stderr | str contains "unauthorized") "the error should mention that the request is unauthorized"

# Read allows reading but not the write operation of creating a subgroup.
tg --token $alice grant $carol_user.id read team
tg --token $carol group get team
let carol_write = tg --token $carol group create team/carol-sub | complete
failure $carol_write "read permission should not allow creating a subgroup"
assert ($carol_write.stderr | str contains "unauthorized") "the error should mention that the request is unauthorized"
# Read does not imply admin, so a read user cannot list grants.
let carol_output = tg --token $carol grants list --resource team | complete
failure $carol_output "read permission should not allow listing grants"
assert ($carol_output.stderr | str contains "unauthorized") "the error should mention that the request is unauthorized"
