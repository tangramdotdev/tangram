use ../../test.nu *
use ../lib/permissions.nu *

# A group admin grant allows inspecting and managing grants, including granting write to another user who can then put and delete tags.

let server = spawn --config { authentication: true }
let users = login_users
let alice = $users.alice
let bob = $users.bob
let carol = $users.carol
let path = artifact 'test'

tg --token $alice group create alice/project

tg --token $alice grant $users.bob_id admin alice/project
tg --token $bob grants list alice/project
tg --token $bob user grants carol
tg --token $bob grant $users.carol_id write alice/project
let carol_id = tg --token $carol checkin $path
tg --token $carol tag put alice/project/carol $carol_id
tg --token $carol tag delete alice/project/carol
