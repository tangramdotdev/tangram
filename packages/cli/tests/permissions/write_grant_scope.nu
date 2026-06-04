use ../../test.nu *
use ../lib/permissions.nu *

# A group write grant allows creating child groups and putting tags, but does not allow inspecting grants, managing grants, or viewing another user's grants.

let server = spawn --config { authentication: true }
let users = login_users
let alice = $users.alice
let bob = $users.bob
let path = artifact 'test'
let id = tg --token $alice checkin $path

tg --token $alice group create alice/project
tg --token $alice group get alice/project

let output = tg --token $bob group create alice/project/bob | complete
assert_unauthorized $output "Bob should not be able to create a child group without write permission."

let output = tg --token $bob tag put alice/project/pkg $id | complete
assert_unauthorized $output "Bob should not be able to put a tag without write permission."

tg --token $alice grant $users.bob_id write alice/project
tg --token $bob group get alice/project
let bob_id = tg --token $bob checkin $path
tg --token $bob tag put alice/project/pkg $bob_id

let output = tg --token $bob grants list alice/project | complete
assert_unauthorized $output "Write permission should not allow Bob to inspect group grants."

let bob_grants = tg --token $bob user grants bob | from json
assert (($bob_grants | length) > 0) "Bob should be able to inspect his own grants."

let output = tg --token $bob user grants carol | complete
assert_unauthorized $output "Write permission should not allow Bob to inspect Carol's grants."

let output = tg --token $bob grant $users.carol_id read alice/project | complete
assert_unauthorized $output "Write permission should not allow Bob to manage grants."
