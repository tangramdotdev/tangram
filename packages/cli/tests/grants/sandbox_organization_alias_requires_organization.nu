use ../../test.nu *

# The --organization owner alias resolves only an organization; a group or a user is rejected.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token organization create acme
tg --token $alice.token organization members add acme $bob.user.id
tg --token $alice.token group create team

let acme = tg --token $bob.token organization get acme | from json

# An organization resolves and is stored as the owner.
let sandbox = tg --token $bob.token sandbox create --organization acme --no-network | str trim
let data = tg --token $bob.token sandbox get $sandbox | from json
assert equal $data.owner $acme.id "the --organization alias should resolve an organization owner"

# A group is not an organization, so --organization must reject it.
let group = tg --token $bob.token sandbox create --organization team --no-network | complete
failure $group "--organization must reject a group"
snapshot ($group.stderr | redact) '
	error an error occurred
	-> failed to resolve the owner as an organization

'

# A user is not an organization, so --organization must reject it.
let user = tg --token $bob.token sandbox create --organization $alice.user.id --no-network | complete
failure $user "--organization must reject a user"
snapshot ($user.stderr | redact) '
	error an error occurred
	-> the owner is not an organization

'
