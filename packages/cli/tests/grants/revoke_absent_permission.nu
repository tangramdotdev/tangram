use ../../test.nu *

# Revoking a permission the grant does not hold fails and leaves the existing permissions intact.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team
tg --token $alice.token grant $bob.user.id read team

# Revoking write, which was never granted, finds nothing to remove.
let output = tg --token $alice.token revoke $bob.user.id write team | complete
failure $output "revoking a permission that was never granted should fail"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to find the grant

'

# The read grant is untouched, so bob can still read the group.
let grants = tg --token $alice.token grants list --resource team | from json
assert ($grants | any {|g| $g.principal == $bob.user.id and $g.permissions == "group_read" }) "the read grant should survive the failed revoke"
tg --token $bob.token group get team
