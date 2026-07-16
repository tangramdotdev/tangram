use ../../test.nu *

# Membership confers write but not admin, so a member cannot remove another member.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let carol = tg login --verbose carol | from json

tg --token $alice.token group create team
tg --token $alice.token group members add team $bob.user.id
tg --token $alice.token group members add team $carol.user.id

# Bob's membership grants him write, so he can create a subgroup under the team.
tg --token $bob.token group create team/bob-sub

# Write does not confer admin, so bob cannot remove another member.
let output = tg --token $bob.token group members remove team $carol.user.id | complete
failure $output "a member without admin should not be able to remove another member"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to remove the group member
	   group = team
	   member = <user>
	-> the request failed
	   status = 500 Internal Server Error
	-> unauthorized

'
