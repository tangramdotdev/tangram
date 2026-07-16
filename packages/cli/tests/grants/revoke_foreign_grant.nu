use ../../test.nu *

# An admin cannot revoke a grant another grantor created; revocation only affects the actor's own grant.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let carol = tg login --verbose carol | from json

tg --token $alice.token group create team
tg --token $alice.token grant $carol.user.id admin team

# Carol grants Bob write on the team.
tg --token $carol.token grant $bob.user.id write team

# Alice, though also an admin, did not create Carol's grant, so her revoke finds nothing of her own to remove.
let output = tg --token $alice.token revoke $bob.user.id write team | complete
failure $output "an admin should not be able to revoke a grant another grantor created"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to find the grant

'

# Carol's grant survives, so Bob retains the write access she conferred.
let grants = tg --token $alice.token grants list --resource team | from json
assert ($grants | any {|g| $g.principal == $bob.user.id and $g.permissions == "group_write" }) "Carol's grant should survive Alice's failed revoke"
tg --token $bob.token group create team/bob-sub
