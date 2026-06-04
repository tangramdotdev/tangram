use ../../test.nu *
use ../lib/permissions.nu *

# Putting a tag auto-creates its group for the owner, and another user cannot put a sibling tag in that auto-created group.

let server = spawn --config { authentication: true }
let users = login_users
let alice = $users.alice
let bob = $users.bob
let path = artifact 'test'
let id = tg --token $alice checkin $path

tg --token $alice tag auto-created/pkg $id
tg --token $alice group get auto-created
tg --token $alice grants list auto-created

let output = tg --token $bob tag auto-created/sibling $id | complete
assert_unauthorized $output "Bob should not be able to put a tag in Alice's auto-created group."
