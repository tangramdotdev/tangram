use ../../test.nu *

# A member of a group that belongs to an organization can access an organization-owned sandbox, and loses access when the group is removed from the organization.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token organization create acme
tg --token $alice.token group create team
tg --token $alice.token group members add team $bob.user.id

let team = tg --token $alice.token group get team | from json
tg --token $alice.token organization members add acme $team.id

let sandbox = tg --token $alice.token sandbox create --organization acme --no-network | str trim

# Bob is a member of the team, and the team belongs to the organization, so he reaches the organization-owned sandbox transitively.
success (tg --token $bob.token sandbox get $sandbox | complete) "a member of a group in the organization should reach the organization-owned sandbox"

# Removing the team from the organization revokes Bob's transitive access.
tg --token $alice.token organization members remove acme $team.id
failure (tg --token $bob.token sandbox get $sandbox | complete) "a member must lose access once their group is removed from the organization"
