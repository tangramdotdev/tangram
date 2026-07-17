use ../../test.nu *

# Adding a user to the owning group after the sandbox is created grants them access to it.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let carol = tg login --verbose carol | from json

tg --token $alice.token group create team
tg --token $alice.token group members add team $bob.user.id

let sandbox = tg --token $bob.token sandbox create --group team --no-network | str trim

# Carol is not yet a member, so she cannot access the team-owned sandbox.
failure (tg --token $carol.token sandbox get $sandbox | complete) "Carol must not get the sandbox before joining the team"

# After Carol joins the team, membership is evaluated live, so she gains access immediately.
tg --token $alice.token group members add team $carol.user.id
success (tg --token $carol.token sandbox get $sandbox | complete) "Carol should get the sandbox after joining the team"
let carol_list = tg --token $carol.token sandbox list | from json
assert (($carol_list | where id == $sandbox | is-empty) == false) "Carol should see the sandbox after joining the team"

tg --token $bob.token sandbox destroy $sandbox
