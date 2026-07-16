use ../../test.nu *

# The --group owner alias resolves only a group; an organization or a user is rejected.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team
tg --token $alice.token grant $bob.user.id write team
tg --token $alice.token organization create acme

let team = tg --token $bob.token group get team | from json

# A group resolves and is stored as the owner.
let sandbox = tg --token $bob.token sandbox create --group team --no-network | str trim
let data = tg --token $bob.token sandbox get $sandbox | from json
assert equal $data.owner $team.id "the --group alias should resolve a group owner"

# An organization is not a group, so --group must reject it.
let organization = tg --token $bob.token sandbox create --group acme --no-network | complete
failure $organization "--group must reject an organization"
snapshot ($organization.stderr | redact) '
	error an error occurred
	-> failed to resolve the owner as a group

'

# A user is not a group, so --group must reject it.
let user = tg --token $bob.token sandbox create --group $alice.user.id --no-network | complete
failure $user "--group must reject a user"
snapshot ($user.stderr | redact) '
	error an error occurred
	-> the owner is not a group

'
