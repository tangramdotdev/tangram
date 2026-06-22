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
snapshot ($dave_output.stderr | redact) '
	error an error occurred
	-> failed to list the grants
	-> the request failed
	   status = 500 Internal Server Error
	-> unauthorized

'

# Read allows reading but not the write operation of creating a subgroup.
tg --token $alice.token grant $carol.user.id read team
tg --token $carol.token group get team
let carol_write = tg --token $carol.token group create team/carol-sub | complete
failure $carol_write "read permission should not allow creating a subgroup"
snapshot ($carol_write.stderr | redact) '
	error an error occurred
	-> failed to create the group
	   specifier = team/carol-sub
	-> the request failed
	   status = 500 Internal Server Error
	-> unauthorized

'
# Read does not imply admin, so a read user cannot list grants.
let carol_output = tg --token $carol.token grants list --resource team | complete
failure $carol_output "read permission should not allow listing grants"
snapshot ($carol_output.stderr | redact) '
	error an error occurred
	-> failed to list the grants
	-> the request failed
	   status = 500 Internal Server Error
	-> unauthorized

'
