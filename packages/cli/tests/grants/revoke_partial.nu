use ../../test.nu *

# Revoking one permission from a grant leaves the remaining permissions and their access intact.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team
tg --token $alice.token grant $bob.user.id read,write team

# Revoking only write leaves read on the grant record.
tg --token $alice.token revoke $bob.user.id write team
let grants = tg --token $alice.token grants list --resource team | from json
assert ($grants | any {|g| $g.principal == $bob.user.id and $g.permissions == "group_read" }) "the grant should retain read after revoking write"

# Bob keeps read access but loses the write operation of creating a subgroup.
tg --token $bob.token group get team
let output = tg --token $bob.token group create team/bob-sub | complete
failure $output "revoking write should remove the ability to create a subgroup"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to create the group
	   specifier = team/bob-sub
	-> the request failed
	   status = 500 Internal Server Error
	-> unauthorized

'
